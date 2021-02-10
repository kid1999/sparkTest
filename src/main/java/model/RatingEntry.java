package model;

import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;

/**
 * @author kid1999
 * @create 2021-02-10 10:22
 * @description ALS计算对象（userid,goodsid,score,time）
 **/

public class RatingEntry implements Serializable {
    private int userId;
    private int goodsId;
    float rating;

    public RatingEntry(int userId, int goodsId, float rating) {
        this.userId = userId;
        this.goodsId = goodsId;
        this.rating = rating;
    }

    public int getUserId() {
        return userId;
    }

    public int getGoodsId() {
        return goodsId;
    }

    public float getRating() {
        return rating;
    }


    public static Rating parseRating(String s){
        String[] fields = s.split(",");
        if(fields.length != 3){
            throw new IllegalArgumentException("Each line must contain 3 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int goodsId = Integer.parseInt(fields[1]);
        float rating = Float.parseFloat(fields[2]);
        return new Rating(userId,goodsId,rating);
    }
}