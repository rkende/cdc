package com.ibm.cdc.userexits;

import java.util.Random;

import com.datamirror.ts.target.publication.userexit.ReplicationEventIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventPublisherIF;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.UserExitIF;

public class UserExitWait implements UserExitIF {
    @Override
    public void init(ReplicationEventPublisherIF p_Publisher) throws UserExitException {
    }

    @Override
    public boolean processReplicationEvent(ReplicationEventIF p_Event) throws UserExitException {

        boolean retValue = true;
        // Generate a random delay between 5 and 10 seconds
        Random random = new Random();
        int delay = 5000 + random.nextInt(5000); // 5000ms (5s) to 10000ms (10s)

        try {
            System.out.println("Waiting for " + delay / 1000 + " seconds...");
            Thread.sleep(delay); // Wait for the random delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            throw new UserExitException("Thread was interrupted during sleep", e);
        }
        return retValue;
    }

    @Override
    public void finish() {
    }
}
