package com.wba.returns.service.business.impl.tote.create;

import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

import configuration.Configuration;

public class WindowSizeLimits {
    int numRequestsPerWindow;
    long windowSizeInMillis;
}

public class RequestDetails {
    String Ip;
    String user;
    String experience;
}

public class RateLimiter {
    ConcurrentHashMap<String, WindowSizeLimits> iprules;
    ConcurrentHashMap<String, WindowSizeLimits> experienceLimits;
    ConcurrentHashMap<String, WindowSizeLimits> userLimits;
    ConcurrentHashMap<String, Deque<Long>> window;

    public boolean isAllowed(RequestDetails requestDetails, long currentTimeMillis) {

        iprules.computeIfAbsent(requestDetails.ip, k -> new WindowSizeLimits(Configuration.getIpLimit(requestDetails.Ip));
        experienceLimits.computeIfAbsent(requestDetails.experience, k -> new WindowSizeLimits(Configuration.getExperienceLimit(requestDetails.experience));
        userLimits.computeIfAbsent(requestDetails.user, k -> new WindowSizeLimits(Configuration.getUserLimits(requestDetails.user));
        Boolean result = false;

        synchronized (window) {
            //for each detail in requestDetails, check if the number of requests in the window is less than the limit
            // and if yes, result should be true, else false
            // and then slide the  Deque<Integer> as adequate. Deque<Long> holds the timestamps of the requests in the window
        }

        return result;
    }

}
