package com.hmdp.service.impl;

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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Collections;
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
    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();
    @PostConstruct  // 在类初始化时执行该方法
    private void init() {
        // 启动线程池，执行任务
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

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
    }

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
                Long result = stringRedisTemplate.execute(
                        VoucherOrderServiceImpl.SECKILL_SCRIPT,
                        Collections.emptyList(),
                        voucherId.toString(),
                        UserHolder.getUser().getId().toString()
                );
                //判断结果是否为0
                int r = result.intValue();
                if (r != 0) {
                    //不为0，说明没有下单资格，返回错误信息
                    return Result.fail(r == 1 ? "库存不足" : "用户已下单");
                }
                //为0，说明有下单资格，把下单信息保存到阻塞队列
                Long orderId = redisIdWorker.nextId("order");
                //保存下单信息到阻塞队列
                //创建订单
                VoucherOrder voucherOrder = new VoucherOrder();

                voucherOrder.setId(redisIdWorker.nextId("order"));
                voucherOrder.setUserId(userId);
                voucherOrder.setVoucherId(voucherId);
                proxy = (IVoucherOrderService) AopContext.currentProxy();
                //将订单添加到阻塞队列
                orderTasks.add(voucherOrder);
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
