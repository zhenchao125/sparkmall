package com.atguigu.sparkmall

package object offline {
    def isNotEmpty(text: String): Boolean = text != null && text.length != 0

    def isEmpty(text: String): Boolean = !isNotEmpty(text)
}
