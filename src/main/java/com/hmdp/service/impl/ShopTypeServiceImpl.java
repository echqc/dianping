package com.hmdp.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        // 从redis中查询店铺类型数据
        ListOperations<String, String> ops = stringRedisTemplate.opsForList();
        List<ShopType> shopTypeList;
        // 0到-1表示查询List中所有元素
        List<String> shopTypeJsonList = ops.range(RedisConstants.CACHE_SHOP_TYPE_KEY, 0, -1);
        // 判断缓存是否命中
        if (CollUtil.isNotEmpty(shopTypeJsonList)) {
            // 缓存命中，直接返回缓存数据
            shopTypeList = shopTypeJsonList.stream()
                    // 将 List<String> 转换为 List<ShopType> 返回
                    .map((shopTypeJson) -> JSONUtil.toBean(shopTypeJson, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(shopTypeList);
        }
        // 缓存未命中，查询数据库
        shopTypeList = this.query().orderByAsc("sort").list();
        // 判断数据库中是否存在该数据
        if (shopTypeList == null) {
            // 数据库中不存在该数据，返回失败信息
            return Result.fail("店铺类型不存在");
        }
        // 数据库中的数据存在，写入Redis，并返回查询的数据
        ops.rightPushAll(RedisConstants.CACHE_SHOP_TYPE_KEY, shopTypeList.stream().map(JSONUtil::toJsonStr).collect(Collectors.toList()));
        // 设置key的过期时间
        stringRedisTemplate.expire(RedisConstants.CACHE_SHOP_TYPE_KEY, RedisConstants.CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);
        // 将数据库查到的数据返回
        return Result.ok(shopTypeList);
    }
}
