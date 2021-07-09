package org.apache.bookkeeper.util;

import org.apache.bookkeeper.client.AsyncCallback;

import java.util.concurrent.FutureTask;

public class RecoverCallback implements AsyncCallback.RecoverCallback {
    private FutureTask<Object> ft;

    public RecoverCallback(FutureTask<Object> ft) {
        this.ft = ft;
    }

    @Override
    public void recoverComplete(int rc, Object ctx) {
        System.out.println("RETURN: " + rc);
        System.out.println("CONTEXT: " + ctx);

        ft.run();
    }
}
