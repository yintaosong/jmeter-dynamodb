package jmeter.dynamodb;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

public class DynamoDBSampler extends AbstractJavaSamplerClient implements Serializable {

    private static final Logger LOG = LoggingManager.getLoggerForClass();

    private static final String PARAM_AWS_ACCESS_KEY_ID = "AWSAccessKeyID";
    private static final String PARAM_AWS_SECRET_KEY = "AWSSecretKey";
    private static final String PARAM_DYNAMO_TABLE_NAME = "DynamoTableName";
    private static final String PARAM_HASH_KEY_NAME= "HashKeyName";
    private static final String PARAM_HASH_KEY_VALUE= "HashKeyValue";
    private static final String PARAM_HASH_KEY_JAVA_CLASS= "HashKeyJavaClass";

    private String hashKeyName;
    private String hashKeyClass;
    private Table table;

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument(PARAM_DYNAMO_TABLE_NAME, null);
        params.addArgument(PARAM_AWS_ACCESS_KEY_ID, null);
        params.addArgument(PARAM_AWS_SECRET_KEY, null);
        params.addArgument(PARAM_HASH_KEY_NAME, null);
        params.addArgument(PARAM_HASH_KEY_VALUE, null);
        params.addArgument(PARAM_HASH_KEY_JAVA_CLASS, "String");
        return params;
    }

    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        String valueStr = context.getParameter(PARAM_HASH_KEY_VALUE);

        if (StringUtils.isEmpty(valueStr)) {
            throw new IllegalArgumentException("Hash key value cannot be null.");
        }

        Object value;

        if ("int".equalsIgnoreCase(hashKeyClass) || "integer".equalsIgnoreCase(hashKeyClass)) {
            value = Integer.valueOf(valueStr);
        } else  if ("long".equalsIgnoreCase(hashKeyClass)) {
            value = Long.valueOf(valueStr);
        } else {
            value = String.valueOf(valueStr);
        }

        Item item = table.getItem(hashKeyName, value);

        result.setResponseData(item.toJSON(), "UTF-8");
        result.setResponseOK();
        result.sampleEnd();
        return result;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        // pull parameters
        String accessKeyId = context.getParameter( PARAM_AWS_ACCESS_KEY_ID );
        String secretKey = context.getParameter( PARAM_AWS_SECRET_KEY );
        String tableName = context.getParameter(PARAM_DYNAMO_TABLE_NAME);
        hashKeyName = context.getParameter(PARAM_HASH_KEY_NAME);
        hashKeyClass = context.getParameter(PARAM_HASH_KEY_JAVA_CLASS, "String");

        AmazonDynamoDBClient dynamoDBClient;
        if (StringUtils.isNotEmpty(accessKeyId) && StringUtils.isNotEmpty(secretKey)) {
            dynamoDBClient = new AmazonDynamoDBClient(new BasicAWSCredentials(accessKeyId, secretKey));
        } else {
            dynamoDBClient = new AmazonDynamoDBClient();
        }
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        table = dynamoDB.getTable(tableName);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }


}
