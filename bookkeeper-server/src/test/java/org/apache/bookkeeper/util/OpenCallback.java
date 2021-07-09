package org.apache.bookkeeper.util;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.LedgerHandle;

import java.util.concurrent.FutureTask;

public class OpenCallback implements AsyncCallback.OpenCallback {
    private LedgerHandle ledgerHandle;
    private FutureTask<Object> ft;

    public OpenCallback(FutureTask<Object> ft) {
        this.ft = ft;
    }

    @Override
    public void openComplete(int rc, LedgerHandle lh, Object ctx) {
        this.ledgerHandle = lh;
        System.out.println("RETURN CODE: " + rc);
        ft.run();
    }

    public LedgerHandle getLedgerHandle() {
        return ledgerHandle;
    }
}
