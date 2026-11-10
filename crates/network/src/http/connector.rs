// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! HTTP proxy routing below the destination TLS connection.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use http::{HeaderMap, HeaderValue, Uri, header::USER_AGENT};
use hyper_rustls::{HttpsConnector, MaybeHttpsStream};
use hyper_util::{
    client::{
        legacy::connect::{Connected, Connection, HttpConnector, proxy::Tunnel},
        proxy::matcher::Matcher,
    },
    rt::TokioIo,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tower_service::Service;

use super::{
    HttpClientError,
    timing::{ConnectionProbe, now},
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug)]
pub(super) struct Connector {
    http: HttpConnector,
    proxy_tls: HttpsConnector<HttpConnector>,
    user_agent: Option<HeaderValue>,
    connection_timing: bool,
    pub(super) proxies: Arc<Matcher>,
}

impl Connector {
    pub(super) fn new(
        tls: rustls::ClientConfig,
        proxy: Option<&str>,
        use_system_proxy: bool,
        user_agent: Option<HeaderValue>,
        connection_timing: bool,
    ) -> Result<Self, HttpClientError> {
        let proxies = if let Some(proxy) = proxy {
            let mut url = match url::Url::parse(proxy) {
                Ok(url) if url.has_host() => url,
                Ok(_) | Err(url::ParseError::RelativeUrlWithoutBase) => {
                    url::Url::parse(&format!("http://{proxy}")).map_err(|_| {
                        HttpClientError::InvalidProxy("proxy URL is malformed".into())
                    })?
                }
                Err(_) => {
                    return Err(HttpClientError::InvalidProxy(
                        "proxy URL is malformed".into(),
                    ));
                }
            };

            if url.port().is_none()
                && matches!(url.scheme(), "socks4" | "socks4a" | "socks5" | "socks5h")
            {
                let _ = url.set_port(Some(1080));
            }

            let matcher = Matcher::builder().all(url.as_str()).build();
            if matcher
                .intercept(&Uri::from_static("http://localhost/"))
                .is_none()
            {
                return Err(HttpClientError::InvalidProxy(
                    "proxy URL is malformed".into(),
                ));
            }

            matcher
        } else if use_system_proxy {
            Matcher::from_system()
        } else {
            Matcher::builder().build()
        };

        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_keepalive(Some(Duration::from_secs(15)));
        http.set_keepalive_interval(Some(Duration::from_secs(15)));
        http.set_keepalive_retries(Some(3));
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        http.set_tcp_user_timeout(Some(Duration::from_secs(30)));
        Ok(Self {
            proxy_tls: HttpsConnector::from((http.clone(), tls)),
            http,
            user_agent,
            connection_timing,
            proxies: Arc::new(proxies),
        })
    }
}

impl Service<Uri> for Connector {
    type Response = TokioIo<Stream>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<TokioIo<Stream>, BoxError>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let proxy = self.proxies.intercept(&dst);
        let mut http = self.http.clone();
        let mut proxy_tls = self.proxy_tls.clone();
        let user_agent = self.user_agent.clone();
        let connection_timing = self.connection_timing;

        Box::pin(async move {
            let started_ns = connection_timing.then(now);
            let Some(proxy) = proxy else {
                return Ok(TokioIo::new(Stream {
                    inner: TokioIo::new(MaybeHttpsStream::Http(http.call(dst).await?)),
                    proxied: false,
                    probe: started_ns.map(|started| ConnectionProbe::new(started, false)),
                }));
            };

            if dst.scheme_str() == Some("https") {
                let mut tunnel = Tunnel::new(proxy.uri().clone(), proxy_tls);
                if let Some(auth) = proxy.basic_auth() {
                    tunnel = tunnel.with_auth(auth.clone());
                }

                if let Some(user_agent) = user_agent {
                    tunnel = tunnel.with_headers(HeaderMap::from_iter([(USER_AGENT, user_agent)]));
                }

                Ok(TokioIo::new(Stream {
                    inner: TokioIo::new(tunnel.call(dst).await?),
                    proxied: false,
                    probe: started_ns.map(|started| ConnectionProbe::new(started, true)),
                }))
            } else {
                Ok(TokioIo::new(Stream {
                    inner: TokioIo::new(proxy_tls.call(proxy.uri().clone()).await?),
                    proxied: true,
                    probe: started_ns.map(|started| ConnectionProbe::new(started, true)),
                }))
            }
        })
    }
}

#[derive(Debug)]
pub(super) struct Stream {
    inner: TokioIo<MaybeHttpsStream<TokioIo<tokio::net::TcpStream>>>,
    proxied: bool,
    probe: Option<ConnectionProbe>,
}

impl Connection for Stream {
    fn connected(&self) -> Connected {
        let connected = self.inner.connected().proxy(self.proxied);
        match &self.probe {
            Some(probe) => connected.extra(probe.clone()),
            None => connected,
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Some(probe) = &this.probe {
            probe.record(false, buf.filled().len() - filled);
        }
        result
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let (Poll::Ready(Ok(bytes)), Some(probe)) = (&result, &this.probe) {
            probe.record(true, *bytes);
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        if let (Poll::Ready(Ok(bytes)), Some(probe)) = (&result, &this.probe) {
            probe.record(true, *bytes);
        }
        result
    }
}

/// Preserves the TLS connector and records its completion separately from TCP or proxy setup.
#[derive(Clone, Debug)]
pub(super) struct TimedConnector(pub(super) HttpsConnector<Connector>);

impl Service<Uri> for TimedConnector {
    type Response = MaybeHttpsStream<TokioIo<Stream>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let future = self.0.call(uri);
        Box::pin(async move {
            let stream = future.await?;
            let mut extras = http::Extensions::new();
            stream.connected().get_extras(&mut extras);
            if let Some(probe) = extras.get::<ConnectionProbe>() {
                probe.established();
            }
            Ok(stream)
        })
    }
}
