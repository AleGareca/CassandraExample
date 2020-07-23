package dao

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder


class KeyspaceRepository(private val session: CqlSession) {
    fun createKeyspace(keyspaceName: String?, numberOfReplicas: Int) {
        val createKeyspace = SchemaBuilder.createKeyspace(keyspaceName!!)
                .ifNotExists()
                .withSimpleStrategy(numberOfReplicas)
        session.execute(createKeyspace.build())
    }

    fun useKeyspace(keyspace: String?) {
        session.execute("USE " + CqlIdentifier.fromCql(keyspace!!))
    }

}