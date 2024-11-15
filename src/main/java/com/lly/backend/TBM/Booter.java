package com.lly.backend.TBM;

import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;


/**
 * Booter类使用.bt文件管理数据库的启动信息
 * 目前的启动信息包括：头表的uid
 */
public class Booter {

    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public Booter(String path, File f) {
        this.path = path;
        this.file = f;
    }


    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Error.error(ErrorItem.FileExistsException);
            }
        } catch (Exception e) {
            Error.error(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Error.error(ErrorItem.FileCannotRWException);
        }
        return new Booter(path, f);

    }

    public static Booter open(String path) {
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Error.error(ErrorItem.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Error.error(ErrorItem.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }


    /**
     * 读取.bt文件内容
     * @return
     */
    public byte[] load(){
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Error.error(e);
        }
        return buf;
    }

    /**
     * 更新.bt文件内容
     * update 在修改 bt 文件内容时，没有直接对 bt 文件进行修改，而是首先将内容写入一个 bt_tmp 文件中，随后将这个文件重命名为 bt 文件。
     * 通过操作系统重命名文件的原子性，来保证操作的原子性。
     */
    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Error.error(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Error.error(ErrorItem.FileCannotRWException);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Error.error(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Error.error(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Error.error(ErrorItem.FileCannotRWException);
        }
    }




}
