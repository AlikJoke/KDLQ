module kdlq.mongo {

    requires transitive kdlq.core;
    requires jsr305;

    requires org.mongodb.driver.sync.client;
    requires org.mongodb.driver.core;
    requires org.mongodb.bson;
    requires kafka.clients;
}