package com.att.cdo.security.silvertail

import org.apache.log4j.{LogManager, Logger}


trait Logging {
    @transient protected lazy val logger: Logger = LogManager.getLogger(getClass.getName)
}