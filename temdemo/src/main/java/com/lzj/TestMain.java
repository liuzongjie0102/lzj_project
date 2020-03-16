package com.lzj;

import sun.security.action.GetPropertyAction;

import java.security.AccessController;

public class TestMain {
    public static void main(String[] args) {
        String conf = "java.security.krb5.conf";


        System.setProperty(conf, "123");
        String var = (String) AccessController.doPrivileged(new GetPropertyAction(conf));
        
        System.out.println(var);
    }
}
