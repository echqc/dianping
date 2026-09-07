package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        //解决缓存穿透问题
      //Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿问题
      //  Shop shop = queryWithMutex(id);
        //逻辑过期时间解决缓存击穿问题
        Shop shop = queryWithLogicExpire(id);

      return Result.ok(shop);
    }
    private Shop queryWithPassThrough(Long id){
        // 从redis中查询商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //存在且有信息直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //存在但没有信息，返回失败
        if(shopJson!=null){
            return null;
        }
        //不存在从数据库中查询
        Shop shop = getById(id);

        //判断是否存在，不存在返回失败
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        //存在保存到redis中
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //返回成功
        return shop;
    }
    public Shop queryWithMutex(Long id){
        // 从redis中查询商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //存在且有信息直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //存在但没有信息，返回失败
        if(shopJson!=null){
            return null;
        }
        //不存在从数据库中查询
         // 获取互斥锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否成功
            if(!isLock){
                Thread.sleep(30);
                return queryWithMutex(id);
            }
            //获取锁成功，从数据库中查询
            shop = getById(id);
            //模拟重建缓存延时
            Thread.sleep(200);

            //判断是否存在，不存在返回失败
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            //存在保存到redis中
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            unlock(lockKey);
        }

        //返回成功
        return shop;


    }
    //逻辑过期解决缓存击穿问题
    // 缓存重建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicExpire(Long id){
        // 从redis中查询商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //不存在直接返回失败
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        //判断是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        //未过期直接返回
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            return shop;
        }
        //过期尝试获取锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        //判断是否成功
        if(isLock){
            shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            // 缓存命中，先把json反序列化为逻辑过期对象
            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            // 将Object对象转成JSONObject再反序列化为目标对象
            shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
            // 判断是否逻辑过期
            if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
                // 命中且未过期，直接返回新数据
                return shop;
            }
            CACHE_REBUILD_EXECUTOR.submit( ()->{

                try{
                    //重建缓存
                    this.saveShop2Redis(id,20L);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }
            });

        }
        //返回商铺信息
        return shop;

    }
    private boolean tryLock(String key){
        Boolean flag= stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 从数据库中查询商铺信息
        Shop shop = getById(id);
        //模拟重建缓存延时
        Thread.sleep(200);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        redisData.setData(shop);
        // 保存到redis中
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));

    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("商铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();

    }
}
