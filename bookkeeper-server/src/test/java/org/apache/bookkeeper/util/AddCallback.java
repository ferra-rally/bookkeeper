package org.apache.bookkeeper.util;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.LedgerHandle;

import java.util.concurrent.FutureTask;

public class AddCallback implements AsyncCallback.AddCallback {
    private FutureTask<Object> ft;

    public AddCallback(FutureTask<Object> ft) {
        this.ft = ft;
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        ft.run();
    }

    @Override
    public void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, long qwcLatency, Object ctx) {
        AsyncCallback.AddCallback.super.addCompleteWithLatency(rc, lh, entryId, qwcLatency, ctx);
    }
}
