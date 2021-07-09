package org.apache.bookkeeper.util;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import java.util.Enumeration;
import java.util.concurrent.FutureTask;

public class ReadCallback implements AsyncCallback.ReadCallback {

    private final FutureTask<Object> ft;
    private Enumeration<LedgerEntry> entryEnumeration;
    private int rc;

    public ReadCallback(FutureTask<Object> ft) {
        this.ft = ft;
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        entryEnumeration = seq;
        this.rc = rc;
        ft.run();
    }

    public Enumeration<LedgerEntry> getEntryEnumeration() {
        return entryEnumeration;
    }

    public int getRc() {
        return rc;
    }
}
