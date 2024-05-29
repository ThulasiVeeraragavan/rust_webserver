use std::{sync::{mpsc, Arc, Mutex}, thread};

pub struct ThreadPool{
    worker:Vec<Worker>,
    sender:mpsc::Sender<Message>,
}
enum Message{
    NewJob(Job),
    Terminate,
}
type Job=Box<dyn FnOnce()+Send+'static>;
impl ThreadPool{
    pub fn new(size:usize)->ThreadPool {
        assert!(size>0);
        let (sender,receiver)=mpsc::channel();
        let receiver=Arc::new(Mutex::new(receiver));
        let mut worker=Vec::with_capacity(size);

        for id in 0..size{
            worker.push(Worker::new(id,Arc::clone(&receiver)));
        }
        ThreadPool{worker,sender}
    }
    pub fn execute<F>(&self,f:F)
    where
    F:FnOnce()+Send+'static{
        let job=Box::new(f);
        self.sender.send(Message::NewJob(job)).unwrap();
    }
}
impl Drop for ThreadPool{
    fn drop(&mut self) {
        println!("Sending terminate mesage to all workers.");
        for _ in &self.worker{
            self.sender.send(Message::Terminate).unwrap();
        }
        println!("shutting down all workers.");
        for worker in &mut self.worker{
            println!("shutting down worker{}",worker.id);
            if let Some(thread)=worker.thread.take(){
                thread.join().unwrap();
            }
        }
    }
}
struct Worker {
    id:usize,
    thread:Option<thread::JoinHandle<()>>,
}
impl  Worker{
    fn new(id:usize,receiver:Arc<Mutex<mpsc::Receiver<Message>>>)->Worker{
        let thread=thread::spawn(move||loop{
            let message=receiver.lock().unwrap().recv().unwrap();
            match message{
                Message::NewJob(job)=>{
                    println!("worker{}got a job;executing.",id);
                    job();
                }
                Message::Terminate=>{
                    println!("worker{}was told to terminate.",id);
                    break;
                }
            }
        });
        Worker{id,thread:Some(thread)}
    }
}