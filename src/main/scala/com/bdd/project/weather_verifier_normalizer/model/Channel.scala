package com.bdd.project.weather_verifier_normalizer.model;


case class Channel  (id :Int, name:String ,alias:String,value:Double,status:Int,valid:Boolean,
                     description:String ) extends Serializable {
}