package com.yinjz.springbootbigdata.mybatis.mapper;

import com.yinjz.springbootbigdata.mybatis.pojo.Emp;
import org.apache.ibatis.annotations.Param;

/**
 * Date:2021/11/30
 * Author:ybc
 * Description:
 */
public interface CacheMapper {

    Emp getEmpByEid(@Param("eid") Integer eid);

    void insertEmp(Emp emp);

}
