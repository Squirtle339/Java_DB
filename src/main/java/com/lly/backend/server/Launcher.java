package com.lly.backend.server;

import com.lly.backend.DM.DataManager;
import com.lly.backend.TBM.TableManager;
import com.lly.backend.TM.TransactionManager;
import com.lly.backend.VM.VersionManagerImpl;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import org.apache.commons.cli.*;

public class Launcher {
    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");

        CommandLineParser cmdParser = new DefaultParser();
        CommandLine cmd = cmdParser.parse(options, args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            System.out.println("server started...");
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            System.out.println("Created DB at " + cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        //单位就已经两字符了
        if(memStr.length()<2){
            Error.error(ErrorItem.InvalidMemException);
        }

        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch (unit){
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Error.error(ErrorItem.InvalidMemException);
        }
        return DEFALUT_MEM;
    }

    private static void createDB(String path){
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManagerImpl vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);

        tm.close();
        dm.close();
    }


    private static void openDB(String path, long mem){
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManagerImpl vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);

        new Server(port, tbm).start();
    }


}
