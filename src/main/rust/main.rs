
use clap::Parser;
use deadqueue;
use futures;
use log::debug;
use std::collections;
use std::sync;
use tokio;

mod file;
mod helper;

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {

    #[arg(long)]
    input: String,

    #[arg(long)]
    worker_count: usize,

    #[arg(long,default_value="2")]
    worker_count_read: usize
}

fn main ()
{
    env_logger::init ();
    debug! ("foo");

    let args = Cli::parse ();
    log::debug! ("args: {:?}", args);

    let input_info = file::tsv::read_tsv (args.input).expect ("Failed to read input tsv");

    let (tx, _) = tokio::sync::broadcast::channel::<()> (1);
    let tx_cancel = tx.clone ();
    let mut rx_shutdown_queue = tx.subscribe ();
    
    let rt = tokio::runtime::Builder::new_multi_thread ()
        .worker_threads (args.worker_count + 2)
        .enable_all ()
        .build ()
        .unwrap ();

    rt.spawn (async move {
        // We don't need to loop as the only thing we will do is quit
        tokio::select! {
            _ = tokio::signal::ctrl_c () => { tx_cancel.send (()).ok ();debug! ("Got crl-c"); },
        }
    });

    rt.block_on (async {
        let mut tasks_read = Vec::<(String, String)>::new ();
        let mut tasks_seen = collections::HashSet::<(String, String)>::new ();
        for info in input_info
        {
            let task = (info.get (0).expect ("Empty line?").clone (), info.get (1).expect ("Empty line?").clone () );
            if tasks_seen.contains (&task)
            {
                eprintln! ("Skipping task {:?}", task);
            }
            else
            {
                tasks_read.push ( task.clone () );
                tasks_seen.insert ( task );
            }
        }

        let queue_read = sync::Arc::new(deadqueue::limited::Queue::new (tasks_read.len ()));

        for task in tasks_read.iter ().cloned ()
        {
            queue_read.try_push (task).expect ("Failed to add task to queue");
        }

        let counts_map = sync::Arc::new (sync::RwLock::new (collections::HashMap::<String, (String, usize, collections::HashMap<String, usize>)>::new ()));

        let handles_read  = (0..args.worker_count_read).map (|worker| {
            let queue_read_local = queue_read.clone ();
            let tx_cancel_worker_local = tx.clone ();
            let local_counts_map = counts_map.clone ();
            tokio::spawn (async move {
                let mut rx_shutdown = tx_cancel_worker_local.subscribe ();
                loop {
                    match rx_shutdown.try_recv ()
                    {
                        Ok (_) => {
                            debug! ("shutdown worker {}", worker);
                        },
                        Err (_) => {
                            //eprintln! ("rx_shutdown err: {:?}", e);
                        }
                    }

                    if let Some (task) = queue_read_local.try_pop ()
                    {
                        let (total, counts) = file::jellyfish::read_hash_counts (task.1.clone ()).expect ("Failed to read jf file");
                        debug! ("worker[{}] read counts for '{}' ...", worker, task.1.clone ());
                        {
                            let mut counts_map_lock = local_counts_map.write ().expect ("Failed to aquire write lock on counts map");
                            (*counts_map_lock).insert (task.0.clone (), (task.1.clone (), total, counts));
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                worker
            })
        }).collect::<Vec<_>> ();

        futures::future::join_all (handles_read)
        .await
        .into_iter ()
        .map (Result::unwrap)
        .for_each (drop);

        while queue_read.len () > 0
        {
            if let Ok (_) = rx_shutdown_queue.try_recv ()
            {
                debug! ("shutdown queue_read poll");
                return;
            }
            else
            {
                eprintln! ("Waiting for {} read workers to finish...", queue_read.len ());
                tokio::time::sleep (tokio::time::Duration::from_millis (5000)).await;
            }
        }

        loop
        {
            if let Ok (_) = rx_shutdown_queue.try_recv ()
            {
                debug! ("shutdown read wait");
                return;
            }
            else
            {
                {
                    match counts_map.try_read ()
                    {
                        Ok (counts_map_r) => {
                            if counts_map_r.len () < tasks_read.len ()
                            {
                                eprintln! ("counts_map_r.keys: {:?}", counts_map_r.keys ());
                                eprintln! ("{}/{} read tasks finished", counts_map_r.len (), tasks_read.len ());
                            }
                            else
                            {
                                break;
                            }
                        },
                        Err (e) => {
                            debug! ("counts_map len try_read failed: {:?}", e);
                       }
                    }
                }
                tokio::time::sleep (tokio::time::Duration::from_millis (5000)).await;
            }
        }

        let samples = counts_map.read ().expect ("Failed to aquire lock on counts map").keys ().cloned ().collect::<Vec<String>> ();
        let mut tasks = Vec::<(String, String)>::new ();
        for i in 0..samples.len ()
        {
            for j in 0..samples.len ()
            {
                if i < j
                {
                    tasks.push ( (samples[i].clone (),  samples[j].clone () ) );
                }
            }
        }

        let queue = sync::Arc::new(deadqueue::limited::Queue::new (tasks.len ()));

        for task in tasks
        {
            queue.try_push (task).expect ("Failed to add task to queue");
        }

        for worker in 0..args.worker_count
        {
            let queue_local = queue.clone ();
            let tx_cancel_worker_local = tx.clone ();
            let local_counts_map = counts_map.clone ();
            tokio::spawn (async move {
                let mut rx_shutdown = tx_cancel_worker_local.subscribe ();
                loop {
                    match rx_shutdown.try_recv ()
                    {
                        Ok (_) => {
                            debug! ("shutdown worker {}", worker);
                        },
                        Err (_) => {
                            //eprintln! ("rx_shutdown err: {:?}", e);
                        }
                    }

                    let task = queue_local.pop ().await;
                    debug! ("worker[{}] processing task[{:?}] ...", worker, task);

                    let counts_map_r = local_counts_map.read ().expect ("Failed to aquire counts map lock");
                    let (_, an, a) = &(*counts_map_r)[&task.0];
                    let (_, bn, b) = &(*counts_map_r)[&task.1];

                    let sim_a = a.keys ().fold (0, |mut acc, item| {
                        if b.contains_key (item)
                        {
                            acc += b[item];
                        }
                        acc
                    });
                    let sim_b = b.keys ().fold (0, |mut acc, item| {
                        if a.contains_key (item)
                        {
                            acc += a[item];
                        }
                        acc
                    });
                    println! ("{}\t{}\t{}\t{}\t{}\t{}", task.0, task.1, an, sim_a, bn, sim_b);
                }
            });
        }

        while queue.len () > 0
        {
            if let Ok (_) = rx_shutdown_queue.try_recv ()
            {
                debug! ("shutdown queue poll");
                break;
            }
            else
            {
                eprintln! ("Waiting for {} workers to finish...", queue.len ());
                tokio::time::sleep (tokio::time::Duration::from_millis (5000)).await;
            }
        }
        eprintln! ("all done");
    });
}

