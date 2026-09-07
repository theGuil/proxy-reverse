# proxy-reverse

Proxy reverso mínimo em Rust/hyper. Uma rota: `GET /?url_redirect=<URL>` refaz a requisição para a URL a partir do servidor e devolve a resposta em streaming.

```sh
cargo run --release
curl "http://127.0.0.1:8080/?url_redirect=https://example.com/"
```

Ouve em `0.0.0.0:8080`.
