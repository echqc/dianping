package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records. forEach(blog -> {
            this.queryBlogUser(blog);   // 查询blog有关的用户
            this.isBlogLiked(blog); // 查询blog是否被点赞
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("笔记不存在");
        }
        queryBlogUser(blog);
        //判断用户是否点赞过
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user=UserHolder.getUser();
        if(user == null){
            //用户未登录,无需查询是否点赞过
            return;
        }
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.BLOG_LIKED_KEY+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }



    @Override
    public Result likeBlog(Long id) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        //判断用户是否点赞过
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //没点赞过
        if (score == null) {
            // 点赞
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            //更新成功,保存用户到redis
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }
        //点赞过
        else {
            // 取消点赞
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //更新成功,删除用户从redis
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }

        }
        return Result.ok();

    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 查询top5的点赞用户 zrange key 0 4
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        // 判断top5是否为空
        if (CollUtil.isEmpty(top5)) {
            return Result.ok(Collections.emptyList());
        }
        // 解析出其中的用户id
        List<Long> userIds = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String userIdsStr = StrUtil.join(",", userIds);	// 每个userId已逗号分隔
        // 根据用户id查询用户，并转为UserVo集合 WHERE id IN (6, 2, 1) ORDER BY FIELD(id, 6, 2, 1) FIELD函数用于根据指定的顺序对结果进行排序
        List<UserDTO> userVoList = userService.query()
                .in("id", userIds).last("ORDER BY FIELD(id, " + userIdsStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 返回UserVo集合
        return Result.ok(userVoList);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean success = save(blog);
        if(!success){
            return Result.fail("保存失败");
        }
        // 查询笔记作者（当前登录用户）的所有粉丝（注意：之前Redis中存的是某用户关注的人，不是某用户的粉丝）select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.lambdaQuery().eq(Follow::getFollowUserId, user.getId()).list();
        // 推送笔记id给所有粉丝
        for (Follow follow : follows) {
            // 获取粉丝id
            Long fanId = follow.getUserId();
            // 推送笔记id
            String key = RedisConstants.FEED_KEY+ fanId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.FEED_KEY + userId;
        // 查询收件箱（关注推送笔记列表）ZREVRANGEBYSCORE key max min LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 非空判断
        if (CollUtil.isEmpty(typedTuples)) {
            return Result.ok();
        }
        // 解析数据：blogId、minTime（时间戳）、offset
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long minTime = 0L;  // 循环最后一次取出的是最小时间戳
        int os = 1; // 默认初始偏移量为1。表示只有自己是相同的
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 获取blogId
            blogIds.add(Long.valueOf(typedTuple.getValue()));
            // 获取score（时间戳）
            long time = typedTuple.getScore().longValue();
            if (time == minTime) {
                // 当前时间等于最小时间，偏移量+1
                os++;
            }else {
                // 当前时间不等于最小时间，更新覆盖最小时间，重置偏移量为1
                minTime = time;
                os = 1;
            }
        }
        // 根据id查询blog，注意保持blogIds的有序性，封装为blog集合
        String blogIdsStr = StrUtil.join(",", blogIds);
        List<Blog> blogs = query().in("id", blogIds).last("order by field(id, " + blogIdsStr + ")").list();
        for (Blog blog : blogs) {
            // 设置blog有关的用户
            queryBlogUser(blog);
            // 设置blog是否被点赞
            isBlogLiked(blog);
        }
        // 封装为滚动分页结果对象，返回给前端
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

}
