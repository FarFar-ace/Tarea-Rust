// Uso: `cargo run --release [puerto] [hilos]`  (por defecto 8443 y 4)

mod thread_pool;

use rustls::{ServerConfig, ServerConnection, StreamOwned};
use rustls_pemfile::{certs, private_key};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thread_pool::ThreadPool;

const CERT_PATH: &str = "certs/cert.pem";
const KEY_PATH: &str = "certs/key.pem";
const PUBLIC_DIR: &str = "public";

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = args.get(1).and_then(|p| p.parse().ok()).unwrap_or(8443);
    let hilos: usize = args.get(2).and_then(|n| n.parse().ok()).unwrap_or(4);

    asegurar_certificado()?;
    let config = Arc::new(cargar_config_tls()?);

    let listener = TcpListener::bind(("0.0.0.0", port))?;
    let pool = ThreadPool::new(hilos);

    println!("Servidor HTTPS escuchando en https://localhost:{port}  ({hilos} hilos)");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let config = Arc::clone(&config);
                pool.execute(move || {
                    if let Err(e) = atender_cliente(stream, config) {
                        eprintln!("[{}] error: {e}", nombre_hilo());
                    }
                });
            }
            Err(e) => eprintln!("conexión fallida: {e}"),
        }
    }
    Ok(())
}

fn nombre_hilo() -> String {
    std::thread::current().name().unwrap_or("?").to_string()
}



/// Si no hay certificado, genera uno autofirmado para `localhost`.
fn asegurar_certificado() -> std::io::Result<()> {
    if Path::new(CERT_PATH).exists() && Path::new(KEY_PATH).exists() {
        return Ok(());
    }
    println!("Generando certificado autofirmado en ./certs ...");
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    fs::create_dir_all("certs")?;
    fs::write(CERT_PATH, ck.cert.pem())?;
    fs::write(KEY_PATH, ck.key_pair.serialize_pem())?;
    Ok(())
}

fn cargar_config_tls() -> std::io::Result<ServerConfig> {
    let cadena: Vec<_> = certs(&mut BufReader::new(File::open(CERT_PATH)?))
        .collect::<Result<_, _>>()?;
    let clave = private_key(&mut BufReader::new(File::open(KEY_PATH)?))?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "sin clave privada"))?;

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cadena, clave)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}



fn atender_cliente(tcp: TcpStream, config: Arc<ServerConfig>) -> std::io::Result<()> {
    tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(10)))?;

    let conn = ServerConnection::new(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    let mut tls = StreamOwned::new(conn, tcp);

    let (metodo, ruta) = match leer_peticion(&mut tls)? {
        Some(p) => p,
        None => return Ok(()),
    };
    println!("[{}] {metodo} {ruta}", nombre_hilo());

    let (estado, tipo, cuerpo) = if metodo != "GET" {
        ("405 Method Not Allowed", "text/plain; charset=utf-8", b"Metodo no permitido".to_vec())
    } else {
        enrutar(&ruta)
    };

    let cabecera = format!(
        "HTTP/1.1 {estado}\r\nContent-Type: {tipo}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        cuerpo.len()
    );
    tls.write_all(cabecera.as_bytes())?;
    tls.write_all(&cuerpo)?;
    tls.flush()?;
    tls.conn.send_close_notify();
    let _ = tls.flush();
    Ok(())
}


fn leer_peticion<R: Read>(flujo: &mut R) -> std::io::Result<Option<(String, String)>> {
    let mut lector = BufReader::new(flujo);
    let mut primera = String::new();
    if lector.read_line(&mut primera)? == 0 {
        return Ok(None);
    }
 
    for _ in 0..100 {
        let mut linea = String::new();
        if lector.read_line(&mut linea)? == 0 || linea == "\r\n" || linea == "\n" {
            break;
        }
    }
    let mut partes = primera.split_whitespace();
    match (partes.next(), partes.next()) {
        (Some(m), Some(r)) => Ok(Some((m.to_string(), r.to_string()))),
        _ => Ok(None),
    }
}

type Respuesta = (&'static str, &'static str, Vec<u8>);

fn enrutar(ruta: &str) -> Respuesta {
    let ruta = ruta.split('?').next().unwrap_or("/");
    match ruta {
        "/" => html(&format!(
            "<h1>Servidor HTTPS en Rust</h1><p>Atendido por <b>{}</b>.</p>\
             <ul><li><a href=\"/hola\">/hola</a></li><li><a href=\"/info\">/info</a></li>\
             <li><a href=\"/dormir\">/dormir</a> (tarda 3 s)</li>\
             <li><a href=\"/static/ejemplo.html\">/static/ejemplo.html</a></li></ul>",
            nombre_hilo()
        )),
        "/hola" => html(&format!("<h1>Hola desde Rust</h1><p>Hilo: {}</p>", nombre_hilo())),
        "/info" => html(&format!(
            "<h1>Info</h1><p>Hilo: {}</p><p>Protocolo: HTTPS (TLS via rustls)</p>",
            nombre_hilo()
        )),
        "/dormir" => {
            // Simula una tarea lenta: con un pool, otras peticiones siguen siendo atendidas.
            std::thread::sleep(Duration::from_secs(3));
            html(&format!("<h1>Listo</h1><p>Terminé tras 3 s en {}.</p>", nombre_hilo()))
        }
        r if r.starts_with("/static/") => servir_archivo(&r["/static/".len()..]),
        _ => ("404 Not Found", "text/html; charset=utf-8", b"<h1>404</h1>".to_vec()),
    }
}

fn html(cuerpo: &str) -> Respuesta {
    let pagina = format!("<!doctype html><meta charset=\"utf-8\"><title>Rust HTTPS</title>{cuerpo}");
    ("200 OK", "text/html; charset=utf-8", pagina.into_bytes())
}

fn servir_archivo(relativa: &str) -> Respuesta {
    let segura = Path::new(relativa)
        .components()
        .all(|c| matches!(c, Component::Normal(_)));
    if !segura || relativa.is_empty() {
        return ("403 Forbidden", "text/plain; charset=utf-8", b"Prohibido".to_vec());
    }
    let ruta: PathBuf = Path::new(PUBLIC_DIR).join(relativa);
    match fs::read(&ruta) {
        Ok(datos) => ("200 OK", tipo_mime(&ruta), datos),
        Err(_) => ("404 Not Found", "text/html; charset=utf-8", b"<h1>404</h1>".to_vec()),
    }
}

fn tipo_mime(ruta: &Path) -> &'static str {
    match ruta.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css",
        Some("js") => "application/javascript",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
