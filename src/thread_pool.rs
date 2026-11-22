use std::sync::{mpsc, Arc, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

struct Worker {
    id: usize,
    handle: Option<thread::JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0, "el pool necesita al menos 1 hilo");

        let (sender, receiver) = mpsc::channel::<Job>();
        // El Receiver no se puede clonar: se comparte con Arc<Mutex<_>>.
        let receiver = Arc::new(Mutex::new(receiver));

        let workers = (0..size)
            .map(|id| Worker::new(id, Arc::clone(&receiver)))
            .collect();

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(sender) = &self.sender {
            sender.send(Box::new(f)).expect("los workers terminaron");
        }
    }
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let handle = thread::Builder::new()
            .name(format!("worker-{id}"))
            .spawn(move || loop {
                // El lock se suelta al terminar esta línea (el guard es temporal),
                // así otros workers pueden esperar trabajos mientras éste ejecuta el suyo.
                let message = receiver.lock().unwrap().recv();
                match message {
                    Ok(job) => job(),
                    Err(_) => break, // el canal se cerró: apagado ordenado
                }
            })
            .expect("no se pudo crear el hilo");

        Worker {
            id,
            handle: Some(handle),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
                println!("worker-{} apagado", worker.id);
            }
        }
    }
}
