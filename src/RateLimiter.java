import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

enum UserTier
{
    FREE, PREMIUM
}

@Getter
@Setter
@AllArgsConstructor
class User {
    String userId;
    UserTier userTier;
}


interface RateLimitConfig
{

}

@AllArgsConstructor
class TokenBucketConfig implements RateLimitConfig
{
    Long refillWindowInMs;
    Double refillTokens;
    Double maxTokens;
}

@AllArgsConstructor
class WindowConfig implements RateLimitConfig
{
    long maxRequestsAllowed;
    long timeIntervalInMs;
    // this will be used for both sliding window and fixed window for this use case
}

interface RateLimiterStrategy
{
    public boolean allow(User user);
}


@AllArgsConstructor
class FixedWindowRateLimitStrategy implements RateLimiterStrategy
{
    static class WindowData
    {
        Integer totalRequests;
        long startTime;

        public WindowData(int i, long currentTime) {
            totalRequests = i;
            startTime = currentTime;
        }
    }

    WindowConfig windowConfig;

    private final Map<String, WindowData> windowDataMap = new ConcurrentHashMap<>();

    @Override
    public boolean allow(User user)
    {
        var userWindow = windowDataMap.computeIfAbsent(user.userId, window -> new WindowData(0, System.currentTimeMillis()));
         synchronized (userWindow)
         {
             long currentTime = System.currentTimeMillis();
             if(currentTime - userWindow.startTime >= windowConfig.timeIntervalInMs)
             {
                 // we can reset the window
                 userWindow.startTime = currentTime;
                 userWindow.totalRequests = 1;
                 return true;
             }
             if(userWindow.totalRequests < windowConfig.maxRequestsAllowed)
             {
                 userWindow.totalRequests++;
                 return true;
             }
             return false;
         }
    }
}

class SlidingWindowRateLimitStrategy implements RateLimiterStrategy
{
    WindowConfig windowConfig;
    private final Map<String, Deque<Long>> userTimeWindowsMap = new ConcurrentHashMap<>();
    @Override
    public boolean allow(User user)
    {
        Deque<Long> userTimes = userTimeWindowsMap.computeIfAbsent(user.getUserId(), a-> new ArrayDeque<>());
       synchronized (Objects.requireNonNull(userTimes)){
           long currentTime = System.currentTimeMillis();
           long timeCutoff = currentTime - windowConfig.timeIntervalInMs;
           while(!userTimes.isEmpty() && userTimes.peekFirst() <= timeCutoff)
           {
               userTimes.pollFirst();
           }

           if(userTimes.size() < windowConfig.maxRequestsAllowed)
           {
               userTimes.addLast(currentTime);
               return true;
           }
           return false;
       }
    }
}

@AllArgsConstructor
class TokenBucketRateLimitStrategy implements RateLimiterStrategy
{
    TokenBucketConfig tokenBucketConfig;

    @AllArgsConstructor
    static class UserTokens{
        Double tokens;
        Long lastFilledTime;
    }

    private final Map<String, UserTokens> userTokensMap = new ConcurrentHashMap<>();

    @Override
    public boolean allow(User user)
    {
        UserTokens userToken = userTokensMap.computeIfAbsent(user.userId, a-> new UserTokens(tokenBucketConfig.maxTokens, System.currentTimeMillis()));
        synchronized (userToken) {
            refill(userToken);
            if(userToken.tokens > 1)
            {
                userToken.tokens = userToken.tokens-1.0;
                return true;
            }
            return false;
        }

    }

    private void refill (UserTokens userToken)
    {
        Long timeElapsed = System.currentTimeMillis() - userToken.lastFilledTime;
        userToken.lastFilledTime = System.currentTimeMillis();
        Double tokens = (tokenBucketConfig.refillTokens)/tokenBucketConfig.refillWindowInMs * timeElapsed;
        userToken.tokens = Math.min(tokenBucketConfig.maxTokens, userToken.tokens + tokens);
    }
}

class RateLimitService {

    public Map<UserTier, RateLimiterStrategy> rateLimiterStrategyMap = new ConcurrentHashMap<>();
    RateLimitService (FixedWindowRateLimitStrategy fixedWindowRateLimitStrategy, TokenBucketRateLimitStrategy tokenBucketRateLimitStrategy)
    {
        rateLimiterStrategyMap.put(UserTier.FREE, fixedWindowRateLimitStrategy);
        rateLimiterStrategyMap.put(UserTier.PREMIUM, tokenBucketRateLimitStrategy);
    }

    public boolean allow(User user)
    {
        return rateLimiterStrategyMap.get(user.userTier).allow(user);
    }
}

class RateLimitUserConfig
{
    private final Map<String, RateLimitConfig> userRateLimitConfig = new HashMap<>();

    public void addUserConfig(User user, RateLimitConfig rateLimitConfig)
    {
        userRateLimitConfig.put(user.userId, rateLimitConfig);
    }

    public RateLimitConfig getUserConfig(User user) {
        return userRateLimitConfig.get(user.userId);
    }
}


public class RateLimiter {


    
    public static void main (String [] args)
    {
        User user1 = new User("user1", UserTier.FREE);
        User user2 = new User("user2", UserTier.PREMIUM);

        TokenBucketConfig tokenBucket = new TokenBucketConfig(3L, 3.0, 5.0);
        WindowConfig windowBucket = new WindowConfig(2, 1);

        FixedWindowRateLimitStrategy fixedWindowRateLimitStrategy = new FixedWindowRateLimitStrategy(windowBucket);
        TokenBucketRateLimitStrategy tokenBucketRateLimitStrategy = new TokenBucketRateLimitStrategy(tokenBucket);

        RateLimitService rateLimitService = new RateLimitService(fixedWindowRateLimitStrategy,tokenBucketRateLimitStrategy );

        for(int i=0;i<100; i++)
        {
            boolean allowUser1 = rateLimitService.allow(user1);
            boolean allowUser2 = rateLimitService.allow(user2);
            System.out.printf("User 1 %s%n", allowUser1);
            System.out.printf("User 2 %s%n", allowUser2);
        }

    }
}
