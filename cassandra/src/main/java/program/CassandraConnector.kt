package program

import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress

class CassandraConnector {
    var session: CqlSession? = null
        private set

    fun connect(node: String?, port: Int?, dataCenter: String?) {
        val builder = CqlSession.builder()
        builder.addContactPoint(InetSocketAddress.createUnresolved(node, port!!))
        builder.withLocalDatacenter(dataCenter!!)
        session = builder.build()
    }


    fun close() {
        session!!.close()
    }
}
