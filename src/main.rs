use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    // 监听指定地址，等待 TCP 连接进来
    let listener = TcpListener::bind("127.0.0.1:10176").await.unwrap();

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // 第二个被忽略的项中包含有新连接的 `IP` 和端口信息
        let (socket, _) = listener.accept().await.unwrap();
        // 为每一条连接都生成一个新的任务，
        // `socket` 的所有权将被移动到新的任务中，并在那里进行处理

        let dbClone = db.clone();

        tokio::spawn(async move {
            process(socket, dbClone).await;
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};
    use std::collections::HashMap;


    // `mini-redis` 提供的便利函数，使用返回的 `connection` 可以用于从 socket 中读取数据并解析为数据帧
    let mut connection = Connection::new(socket);

    // 使用 `read_frame` 方法从连接获取一个数据帧：一条redis命令 + 相应的数据
    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("frame: {:?}", frame);
        let response: Frame = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();

                // 值被存储为 `Vec<u8>` 的形式
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();

                if let Some(value) = db.get(cmd.key()) {
                    // `Frame::Bulk` 期待数据的类型是 `Bytes`， 该类型会在后面章节讲解，
                    // 此时，你只要知道 `&Vec<u8>` 可以使用 `into()` 方法转换成 `Bytes` 类型
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // 将请求响应返回给客户端
        connection.write_frame(&response).await.unwrap();
    }
}
