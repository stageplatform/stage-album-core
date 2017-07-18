package pl.pwojcik.stage.album.infrastructure;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

/**
 * Created by pwojcik on 2017-07-06.
 */
@Component
public class DynamoConnector {

    public static final String ID_KEY = "id";

    private static final String TABLE_NAME = "album";
    private static final String AWS_KEY = "key";
    private static final String AWS_SECRET = "value";

    private AmazonDynamoDB client;
    private DynamoDB dynamoDB;

    @Value("${dynamo.host}")
    private String dynamoHost;

    @Value("${dynamo.port}")
    private String dynamoPort;

    @Value("${dynamo.region}")
    private String dynamoRegion;

    @EventListener(ContextRefreshedEvent.class)
    public void initialize() {
        client = createClient();
        dynamoDB = new DynamoDB(client);
        if (shouldCreateTable(client, TABLE_NAME)) {
            createTable();
        }
    }

    public AmazonDynamoDB getClient() {
        return client;
    }

    public Table getTable() {
        return dynamoDB.getTable(TABLE_NAME);
    }

    private AmazonDynamoDB createClient() {
        final String dynamoEndpoint = dynamoHost + ":" + dynamoPort;
        final EndpointConfiguration endpointConfig = new EndpointConfiguration(dynamoEndpoint, dynamoRegion);
        final AWSCredentials credentials = new BasicAWSCredentials(AWS_KEY, AWS_SECRET);
        final AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        return AmazonDynamoDBClientBuilder.standard()//
                .withEndpointConfiguration(endpointConfig)//
                .withCredentials(credentialsProvider)//
                .build();
    }

    public boolean shouldCreateTable(final AmazonDynamoDB client, final String tableName) {
        try {
            final TableDescription table = client.describeTable(new DescribeTableRequest(tableName))
                    .getTable();
            return !TableStatus.ACTIVE.toString().equals(table.getTableStatus());
        } catch (final ResourceNotFoundException exception) {
            return true;
        }
    }

    private void createTable() {
        final List<KeySchemaElement> keySchemas = Arrays.asList(//
                new KeySchemaElement(ID_KEY, KeyType.HASH)
        );

        final List<AttributeDefinition> attributeDefinitions = Arrays.asList(//
                new AttributeDefinition(ID_KEY, ScalarAttributeType.S)
        );
        final ProvisionedThroughput throughput = new ProvisionedThroughput(10L, 10L);
        final Table table = dynamoDB.createTable(TABLE_NAME, keySchemas, attributeDefinitions, throughput);
        try {
            table.waitForActive();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }
}
