package model

import java.util.*

class Libro(uuid: UUID, title: String?, author: String?,subject:String?,publicher:String?) {
    val id: UUID? = uuid

    val title = title

     val author = author

     val subject = subject

     val publisher =publicher
}