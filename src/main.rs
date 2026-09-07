//! Proxy reverso mínimo em hyper.
//!
//! Uma única rota: recebe `GET /?url_redirect=<URL>`, refaz a requisição
//! para essa URL a partir do próprio servidor (limpando os headers que
//! identificam o cliente original) e devolve a resposta em streaming.

use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderValue, HOST};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use url::Url;

/// Corpo de resposta usado no proxy: repassa o corpo do upstream sem cópia.
type ProxyBody = http_body_util::combinators::BoxBody<hyper::body::Bytes, hyper::Error>;

/// Headers do cliente que NÃO devem seguir para o upstream, para a
/// requisição sair "como se fosse" do nosso servidor.
const STRIP_HEADERS: &[&str] = &[
    "host",
    "cookie",
    "authorization",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-real-ip",
    "forwarded",
    "via",
    "referer",
    "origin",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Provider de criptografia do rustls (necessário para o TLS do upstream).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    let listener = TcpListener::bind(addr).await?;
    println!("proxy-reverse ouvindo em http://{addr}");
    println!("uso: http://{addr}/?url_redirect=https://alvo.com/path");

    // Conector HTTPS + cliente com pool de conexões keep-alive para o upstream.
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_all_versions()
        .build();
    let client: Client<_, Incoming> = Client::builder(TokioExecutor::new()).build(https);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let client = client.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| proxy(req, client.clone()));
            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
            {
                eprintln!("erro de conexão: {e}");
            }
        });
    }
}

/// Trata a requisição recebida e faz o proxy para a URL de `?url_redirect=`.
async fn proxy(
    req: Request<Incoming>,
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Incoming,
    >,
) -> Result<Response<ProxyBody>, Infallible> {
    match forward(req, client).await {
        Ok(resp) => Ok(resp),
        Err(msg) => Ok(error(StatusCode::BAD_GATEWAY, msg)),
    }
}

/// Extrai a URL alvo, reconstrói a requisição e a envia pelo cliente.
async fn forward(
    req: Request<Incoming>,
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Incoming,
    >,
) -> Result<Response<ProxyBody>, String> {
    // 1. Lê ?url_redirect= da query da requisição recebida.
    let query = req.uri().query().unwrap_or("");
    let target = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("url_redirect="))
        .ok_or("faltou o parâmetro ?url_redirect=")?;
    let target = percent_decode(target);
    let target = Url::parse(&target).map_err(|e| format!("url inválida: {e}"))?;

    if !matches!(target.scheme(), "http" | "https") {
        return Err("apenas http/https são permitidos".into());
    }
    let host = target
        .host_str()
        .ok_or("url sem host")?
        .to_string();

    // 2. Reconstrói a requisição de saída com o método e headers originais,
    //    trocando o destino e limpando os headers do cliente.
    let (mut parts, body) = req.into_parts();
    parts.uri = target
        .as_str()
        .parse()
        .map_err(|e| format!("uri de saída inválida: {e}"))?;

    let mut headers = HeaderMap::new();
    for (name, value) in &parts.headers {
        if !STRIP_HEADERS.contains(&name.as_str()) {
            headers.insert(name, value.clone());
        }
    }
    // Host correto do alvo, para a requisição sair como daquele servidor.
    if let Ok(hv) = HeaderValue::from_str(&host) {
        headers.insert(HOST, hv);
    }
    parts.headers = headers;

    let out_req = Request::from_parts(parts, body);

    // 3. Envia e repassa a resposta em streaming.
    let resp = client
        .request(out_req)
        .await
        .map_err(|e| format!("falha ao contatar o upstream: {e}"))?;

    let (parts, body) = resp.into_parts();
    Ok(Response::from_parts(parts, body.boxed()))
}

/// Resposta de erro simples em texto.
fn error(status: StatusCode, msg: impl Into<String>) -> Response<ProxyBody> {
    let body = http_body_util::Full::new(hyper::body::Bytes::from(msg.into()))
        .map_err(|never: Infallible| match never {})
        .boxed();
    Response::builder().status(status).body(body).unwrap()
}

/// Decode percent-encoding básico (%XX) do valor da query.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
