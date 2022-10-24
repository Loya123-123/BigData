package com.yinjz.spark.core.framework.common

import com.yinjz.spark.core.framework.util.EnvUtil

trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
