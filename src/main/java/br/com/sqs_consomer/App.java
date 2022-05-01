package br.com.sqs_consomer;

import br.com.sqs_consomer.services.SQSService;

public class App {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Lendo mensagem ...");
        while (true) {
            System.out.println("Aguardando Mensagens..");
            SQSService.messageReader();
        }
    }
}
