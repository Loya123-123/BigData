package com.yinjz.flink.evop.tuning.bean;

import com.yinjz.flink.evop.source.config.AppConfig;
import com.yinjz.flink.evop.source.util.ParamUtil;
import com.yinjz.flink.evop.source.util.RanOpt;
import com.yinjz.flink.evop.source.util.RandomNum;
import com.yinjz.flink.evop.source.util.RandomOptionGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppCommonWithDay {

    private String mid; // (String) 设备唯一标识
    private String uid; // (String) 用户uid
    private String vc;  // (String) versionCode，程序版本号
    private String ch;  // (String) 渠道号，应用从哪个渠道来的。
    private String os;  // (String) 系统版本
    private String ar;  // (String) 区域
    private String md;  // (String) 手机型号
    private String ba;  // (String) 手机品牌
    private String is_new; // 是否新用户
    private String day; // 时间 yyyyMMdd

}
