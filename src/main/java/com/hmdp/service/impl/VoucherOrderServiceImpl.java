package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
   @Autowired
   private ISeckillVoucherService seckillVoucherService;
   @Autowired
   private RedisIdWorker redisIdWorker;
   @Autowired
   private StringRedisTemplate stringRedisTemplate;
   @Autowired
   private RedissonClient redissonClient;
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //查询秒杀优惠券信息
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        //判断秒杀是否开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀未开始");
        }
        //判断秒杀是否结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束");
        }
        //判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
        //创建锁对象(新增代码)
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("order:" + userId);
        //获取锁对象
        //boolean isLock = lock.tryLock(1200);
        boolean isLock = lock.tryLock();
        //加锁失败
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            lock.unlock();
        }
    }*/
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    // 静态代码块初始化加载脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();
    @PostConstruct  // 在类初始化时执行该方法
    private void init() {
        // 启动线程池，执行任务
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable {
        private static final String QUEUE_NAME = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.lastConsumed())
                    );
                    // 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明没有消息，继续下一次读取
                        continue;
                    }
                    // 解析消息中的订单信息 MapRecord<消息id, 消息key，消息value>
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id [id1 id2 id3 ...]
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }
        // 处理pending-list中的异常订单信息
        private void handlePendingList() {
            while (true) {  // 不断获取消息队列中的订单信息
                try {
                    // 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.from("0"))
                    );
                    // 判断异常消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明pending-list中没有异常消息，结束循环
                        break;
                    }
                    // 解析消息中的订单信息 MapRecord<消息id, 消息key，消息value>
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 id [id1 id2 id3 ...]
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 防止处理频繁，下次循环休眠20毫秒
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    /*private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    // 线程任务内部类
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                // take()方法：从阻塞队列中获取元素，如果队列为空，线程会被阻塞，直到队列中有元素，线程才会被唤醒，并去获取元素
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 处理订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("order:" + userId);
        boolean isLock = lock.tryLock();
        //加锁失败
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            //获取代理对象(事务)

            proxy.createSecKillVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }

    }
    // 事务代理对象
    private IVoucherOrderService proxy;
            @Override
            public Result seckillVoucher (Long voucherId){
                Long userId = UserHolder.getUser().getId();
                long orderId = redisIdWorker.nextId("order");
                Long result = stringRedisTemplate.execute(
                        SECKILL_SCRIPT,
                        Collections.emptyList(),
                        voucherId.toString(),
                        UserHolder.getUser().getId().toString(),
                        String.valueOf(orderId)
                );
                //判断结果是否为0
                int r = result.intValue();
                if (r != 0) {
                    //不为0，说明没有下单资格，返回错误信息
                    return Result.fail(r == 1 ? "库存不足" : "用户已下单");
                }
                //为0，说明有下单资格，把下单信息保存到阻塞队列
                /*//保存下单信息到阻塞队列
                //创建订单
                VoucherOrder voucherOrder = new VoucherOrder();

                voucherOrder.setId(redisIdWorker.nextId("order"));
                voucherOrder.setUserId(userId);
                voucherOrder.setVoucherId(voucherId);
                //将订单添加到阻塞队列
                orderTasks.add(voucherOrder);*/
                proxy = (IVoucherOrderService) AopContext.currentProxy();
                return Result.ok(orderId);

            }
            @Transactional
            public void createSecKillVoucherOrder (VoucherOrder voucherOrder){
                // 根据用户id和优惠券id查询订单是否存在
                int count = query().eq("user_id", voucherOrder.getUserId()).eq("voucher_id", voucherOrder.getVoucherId()).count();
                // 一人一单判断
                if (count > 0) {
                    // 该用户已经购买过了，不允许下多单
                    log.error("该秒杀券用户已经购买过一次了！");
                    return;
                }
                // 扣减库存
                boolean success = seckillVoucherService.update()
                        .setSql("stock = stock - 1")    // set stoke = stoke - 1
                        // where id = ? and stock > 0
                        .eq("voucher_id", voucherOrder.getVoucherId())
                        .gt("stock", 0)
                        //.eq("stock", seckillVoucher.getStock())   // CAS乐观锁（成功卖出概率太低、需要用 stock > 0 来判断）
                        .update();
                if (!success) {
                    // 扣减失败
                    log.error("扣减失败，秒杀券扣减失败（库存不足）！");
                    return;
                }
                // 将订单信息写入数据库
                success = this.save(voucherOrder);
                if (!success) {
                    // 创建秒杀券订单失败
                    throw new RuntimeException("创建秒杀券订单失败！");
                }
            }
   /* @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //判断用户是否已购买
        int count=query().eq("user_id",userId).eq("voucher_id", voucherId).count();
        if(count>0){
            return Result.fail("该用户已购买过一次");
        }
        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock",0)
                .update();
        if (!success) {
            return Result.fail("扣减库存失败");
        }
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();

        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        success = this.save(voucherOrder);
        if (!success) {
            // 创建秒杀券订单失败
            throw new RuntimeException("创建秒杀券订单失败！");
        }
        return Result.ok(voucherOrder.getId());
    }*/
        }
