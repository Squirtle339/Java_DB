package com.lly.backend.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;


/**
 * 传输层，创建reader和writer和socket的输入输出流相连
 * 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String）
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if(line == null) {
            close();
        }
        return hexDecode(line);
    }

    /**
     * 将编码后的数据转成十六进制字符串发送
     * @param data
     * @throws IOException
     */
    public void send(byte[] data) throws IOException {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        // 为信息末尾加上换行符，方便使用BufferedReader 和 Writer 来直接按行读写
        return Hex.encodeHexString(buf, true)+"\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }


}
