package com.yinjz.springbootbigdata.mybatisplus.service.impl;

import com.yinjz.springbootbigdata.mybatisplus.mapper.UserMapper;
import com.yinjz.springbootbigdata.mybatisplus.pojo.User;
import com.yinjz.springbootbigdata.mybatisplus.service.UserService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * Date:2022/2/13
 * Author:ybc
 * Description:
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {
}
