package br.com.sqs_consomer.services;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import com.google.gson.Gson;

import br.com.sqs_consomer.dto.infoPedido;

public class SQSService {
    public static void messageReader() {
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider() {
            @Override
            public AwsCredentials resolveCredentials() {
                return new AwsCredentials() {
                    @Override
                    public String accessKeyId() {
                        return System.getenv("AWS_ACCESS_KEY");
                    }

                    @Override
                    public String secretAccessKey() {
                        return System.getenv("AWS_SECRET_KEY");
                    }
                };
            }
        };

        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                .build();

        String awsId = System.getenv("AWS_ACCOUNT_ID");
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName("queue_poc_ecommerce")
                .queueOwnerAWSAccountId(awsId).build();
        GetQueueUrlResponse createResult = sqsClient.getQueueUrl(request);

        List<Message> messages = receiveMessages(sqsClient, createResult.queueUrl());

        for (Message mess : messages) {

            var jsonPedido = mess.body();
            infoPedido pedido = new Gson().fromJson(jsonPedido, infoPedido.class);

            String id = "notaFiscal";
            String to = pedido.getTo();

            if (to.equals(id)) {
                System.out.println("APROVACAO , enviando despacho");
                deleteMessages(sqsClient, createResult.queueUrl(), mess);

                infoPedido pedidoOut = new infoPedido(id, "despacho", pedido.getNumPedido());
                sendMessage(sqsClient, createResult.queueUrl(), pedidoOut);

                infoPedido pedidoOut2 = new infoPedido(id, "pedidos", pedido.getNumPedido());
                sendMessage(sqsClient, createResult.queueUrl(), pedidoOut2);
            }
        }
        sqsClient.close();
    }

    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(20)
                .maxNumberOfMessages(5)
                .build();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        return messages;
    }

    public static void deleteMessages(SqsClient sqsClient, String queueUrl, Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    public static void sendMessage(SqsClient sqsClient, String queueUrl, infoPedido message) {
        System.out.println(message);
        String jsonPedido = new Gson().toJson(message);
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(jsonPedido)
                .build();
        sqsClient.sendMessage(sendMsgRequest);
    }
}
