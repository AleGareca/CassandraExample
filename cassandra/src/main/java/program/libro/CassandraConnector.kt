package program.libro

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CassandraConnector {
    private val LOG: Logger = LoggerFactory.getLogger(CassandraConnector::class.java)

    private var cluster: Cluster? = null

    private var session: Session? = null

    fun connect(node: String?, port: Int?) {
        val b = Cluster.builder().addContactPoint(node)
        if (port != null) {
            b.withPort(port)
        }
        cluster = b.build()
        val metadata = cluster!!.metadata
        LOG.info("Cluster name: " + metadata.clusterName)
        for (host in metadata.allHosts) {
            LOG.info("Datacenter: " + host.datacenter + " Host: " + host.address + " Rack: " + host.rack)
        }
        session = cluster!!.connect()
    }

    fun getSession(): Session? {
        return session
    }

    fun close() {
        session!!.close()
        cluster!!.close()
    }
}