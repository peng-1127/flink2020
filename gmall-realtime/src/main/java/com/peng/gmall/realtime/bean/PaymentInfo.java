package com.peng.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author bp
 * @create 2021-07-02 18:07
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}