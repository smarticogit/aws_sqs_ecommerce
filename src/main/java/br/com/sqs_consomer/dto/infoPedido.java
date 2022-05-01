package br.com.sqs_consomer.dto;

public class infoPedido {
    private String from;
    private String to;
    private Integer numPedido;

    public infoPedido() {
    }

    public infoPedido(String from, String to, Integer numPedido) {
        this.from = from;
        this.to = to;
        this.numPedido = numPedido;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Integer getNumPedido() {
        return numPedido;
    }

    public void setNumPedido(Integer numPedido) {
        this.numPedido = numPedido;
    }

    @Override
    public String toString() {
        return "Pedido from: " + from + ", to: " + to +" numPedido: " + numPedido + "";
    }
}
