package com.lly.backend.server;

import com.lly.backend.TBM.TableManager;
import com.lly.backend.transport.Encoder;
import com.lly.backend.transport.Package;
import com.lly.backend.transport.Packager;
import com.lly.backend.transport.Transporter;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Array;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Server {
    private int port;
    TableManager tbm;
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        // 创建线程池, 10个核心线程, 20个最大线程, 1秒空闲回收,容量100的阻塞队列, 拒绝策略为CallerRunsPolicy
        ThreadPoolExecutor tpe= new ThreadPoolExecutor(10,20,1L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try{
            while(true) {
                Socket socket = serverSocket.accept();
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }


        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                serverSocket.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    class HandleSocket implements Runnable {
        private Socket socket;
        private TableManager tbm;
        public HandleSocket(Socket socket, TableManager tbm) {
            this.socket = socket;
            this.tbm = tbm;
        }

        @Override
        public void run() {
            InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
            System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
            Packager packager = null;
            try{
                // 创建传输器
                Transporter transporter = new Transporter(socket);
                // 创建编码器
                Encoder encoder = new Encoder();
                // 创建打包器
                packager = new Packager(transporter, encoder);
            }catch (Exception e){
                e.printStackTrace();
                try {
                    socket.close();
                }catch (Exception e1){
                    e1.printStackTrace();
                }
            }

            Executor exe = new Executor(tbm);
            while (true){
                Package pkg = null;
                try {
                    //socket接收数据
                    pkg = packager.receive();
                } catch(Exception e) {
                    break;
                }
                byte[] sql = pkg.getData();

                byte[] result = null;
                Exception e = null;
                try {
                    //执行sql
                    result = exe.execute(sql);
                } catch (Exception e1) {
                    e = e1;
                    e.printStackTrace();
                }

                pkg = new Package(result, e);
                try {
                    //发送数据
                    packager.send(pkg);
                } catch (Exception e1) {
                    e1.printStackTrace();
                    break;
                }
            }
            exe.close();
            try {
                packager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }
}
