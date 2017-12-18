import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.PCA

//Reading Members data
val members = spark.read.option("header","true").option("inferSchema","true").csv("s3://s3kkbox/members_v3.csv")

val indexer = new StringIndexer()
  .setInputCol("registered_via")
  .setOutputCol("registered_via_index")
  .fit(members)

 val indexer1= indexer.transform(members)
 
val indexer2 = new StringIndexer()
  .setInputCol("city")
  .setOutputCol("city_index")
  .fit(indexer1)

val indexer3= indexer2.transform(indexer1).toDF
  
val training_AGE_0_TO_100 = indexer3.where("bd between 1 and 100")
val outliers = indexer3.where("bd == 0 OR bd< 0 OR bd> 100")


val indexer4 = new StringIndexer()
  .setInputCol("bd")
  .setOutputCol("bd_index")
  .fit(training_AGE_0_TO_100)

val indexer5= indexer4.transform(training_AGE_0_TO_100).toDF.cache
indexer5.show()

val assembler = new VectorAssembler()
  .setInputCols(Array("city_index", "registered_via_index"))
  .setOutputCol("features")
 val output = assembler.transform(indexer5)
 
 output.select(output("bd")).distinct.count
 
 val dt = new DecisionTreeClassifier()
  .setLabelCol("bd_index")
  .setFeaturesCol("features")
 
val pipeline = new Pipeline()
  .setStages(Array(assembler, dt))

val model = pipeline.fit(indexer5)

val AGE_PRED = model.transform(outliers)
//AGE_PRED.select("prediction","bd").show()

AGE_PRED.sort(desc("bd")).show()

val outlier_predicted = AGE_PRED.drop("features", "bd","rawPrediction","probability").withColumnRenamed("prediction", "bd")


val train_members=indexer5.drop("bd").withColumnRenamed("bd_index", "bd")

val members_final= outlier_predicted.unionAll(train_members)
//members_final.show()
members.count()
members_final.count()  

//Reading transcations data
val trans = spark.read.option("header","true").option("inferSchema","true").csv("s3://s3kkbox/transactions.csv").cache
//trans.count
//trans.show()

val order_trans = trans.orderBy(asc("transaction_date")).cache
val rows_trans = order_trans.rdd.cache
//rows_trans.take(10)

val mapd_trans = rows_trans.map(x => (x(0).toString,(x(6).toString.toDouble, x(7).toString.toDouble, x(8).toString.toDouble))).cache
//mapd_trans.take(10)


val redcd_trans = mapd_trans.groupByKey.mapValues(_.toList)
redcd_trans.cache

//redcd_trans.take(10)

//Analyze the transcations of the user and predict his behaviour
def compareTuples (a:List[(Double,Double,Double)]): Int = {
   val count =0
   val count1 =1
   var diff =0.0
   for(x<-2 to a.length)
   {
     diff = a(x-1)._1 - a(x-2)._2
     if((diff>100 && diff<8870) ||( diff>=8900))
       
       { return count1}
      
   }
   return count
}

var result_trans=redcd_trans.map(x=>(x._1,compareTuples(x._2))).cache
//result_trans.take(10)
//result_trans.filter(x=>x._2==1).count

val behvr = result_trans.toDF("msno": String, "tran_behvr": String).cache
//behvr.show()

//Reading User logs data
val user = spark.read.option("header","true").option("inferSchema","true").csv("s3://s3kkbox/user_logs.csv").cache

//user.count
val user_grp = user.groupBy("msno").agg($"msno",sum("num_25"),sum("num_50"),sum("num_75"),sum("num_985"),sum("num_100"),sum("num_unq"),sum("total_secs")).cache

val train = spark.read.option("header","true").option("inferSchema","true").csv("s3://s3kkbox/train.csv").cache


val join=user_grp.join(train,"msno").cache

val join_25=join.select("sum(num_25)").map(r => r(0).toString.toDouble).rdd.cache
val join_50=join.select("sum(num_50)").map(r => r(0).toString.toDouble).rdd.cache
val join_75=join.select("sum(num_75)").map(r => r(0).toString.toDouble).rdd.cache
val join_985=join.select("sum(num_985)").map(r => r(0).toString.toDouble).rdd.cache
val join_100=join.select("sum(num_100)").map(r => r(0).toString.toDouble).rdd.cache
val join_unq=join.select("sum(num_unq)").map(r => r(0).toString.toDouble).rdd.cache
val join_secs=join.select("sum(total_secs)").map(r => r(0).toString.toDouble).rdd.cache
val join_churn=join.select("is_churn").map(r => r(0).toString.toDouble).rdd.cache

val correlation_25: Double = Statistics.corr(join_25,join_churn, "pearson")
//println(s"Correlation is: $correlation_25")

val correlation_50: Double = Statistics.corr(join_50,join_churn, "pearson")

val correlation_75: Double = Statistics.corr(join_75,join_churn, "pearson")

val correlation_985: Double = Statistics.corr(join_985,join_churn, "pearson")

val correlation_100: Double = Statistics.corr(join_100,join_churn, "pearson")

val correlation_unq: Double = Statistics.corr(join_unq,join_churn, "pearson")

val correlation_secs: Double = Statistics.corr(join_secs,join_churn, "pearson")

members_final.printSchema()

//Joining user logs,members,behvr data 
val join1=user_grp.join(members_final,"msno").cache
val join2=join1.join(behvr,"msno").cache
join2.printSchema()

//Join with train data
val train_df=train.join(join2,"msno").cache
train_df.printSchema()
//train_df.count()

val traindf=train_df.drop("gender","registered_via","city","registration_init_time").cache

//Splitting the data in train and test set
val Array(trainset, testset) = traindf.randomSplit(Array(0.8, 0.2))

trainset.show

//Convert columns to feature vector
val assembler_train = new VectorAssembler().setInputCols(Array("sum(num_25)","sum(num_50)","sum(num_75)","sum(num_985)","sum(num_100)","sum(num_unq)","sum(total_secs)","registered_via_index","city_index","bd","tran_behvr")).setOutputCol("features")

//PCA
val assm_train = assembler_train.transform(traindf)

val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(4).fit(assm_train)

//Split the data 80:20 into train and test
val result_pca = pca.transform(assm_train)
val Array(trainsetp, testsetp) = result_pca.randomSplit(Array(0.8, 0.2))

//Logistic regression model

val lr = new LogisticRegression()
  .setMaxIter(5)
  .setRegParam(0.5)
  .setElasticNetParam(0.8)
  .setLabelCol("is_churn")
  .setFamily("multinomial")
  
//Build pipeline
val pipeline_lr = new Pipeline()
  .setStages(Array(assembler_train,lr))
 //Build Paramgrid 
  val paramGrid_lr = new ParamGridBuilder()
  .addGrid(lr.threshold, Array(0.1,0.5,0.6))
  .addGrid(lr.regParam, Array(0.1,0.001,0.0001))
  .addGrid(lr.maxIter, Array(10,20,30))
  .build()
val evaluator_lr = new MulticlassClassificationEvaluator().setLabelCol("is_churn")
//Build cross validator with 5 folds
val cv_lr = new CrossValidator()
  .setEstimator(pipeline_lr)
  .setEstimatorParamMaps(paramGrid_lr)
  .setEvaluator(evaluator_lr)
  .setNumFolds(5)
  
  //Building model 
val model_train_lr = cv_lr.fit(trainset)
//Applying the model on test data
val result_lr = model_train_lr.transform(testset)
//Selecting prediction and label columns
val predictionAndLabels_lr = result_lr.select("rawPrediction","prediction", "is_churn")

println("Evaluation of Logistic Regression model without dimensionality reduction")
val evaluator_lr = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision").setLabelCol("is_churn")
println("Precision:" + evaluator_lr.evaluate(predictionAndLabels_lr))
val evaluator1_lr = new MulticlassClassificationEvaluator().setMetricName("weightedRecall").setLabelCol("is_churn")
println("Recall:" + evaluator1_lr.evaluate(predictionAndLabels_lr))


//Logistic Regeression
val lr_pca = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.5)
  .setThreshold(0.5)
  .setElasticNetParam(0.8)
  .setLabelCol("is_churn")
  .setFamily("multinomial")
val pipeline_lr_pca = new Pipeline()
  .setStages(Array(lr_pca))
val paramGrid_lr_pca = new ParamGridBuilder()
  .addGrid(lr_pca.threshold, Array(0.1,0.5,0.6))
  .addGrid(lr_pca.regParam, Array(0.1,0.2,0.01))
  .addGrid(lr_pca.maxIter, Array(10,25,30))
  .build()
val evaluator_lr_pca = new BinaryClassificationEvaluator().setLabelCol("is_churn")
val cv_lr_pca = new CrossValidator()
  .setEstimator(pipeline_lr_pca)
  .setEstimatorParamMaps(paramGrid_lr_pca)
  .setEvaluator(evaluator_lr_pca)
  .setNumFolds(5)
 
val model_lr_pca = cv_lr_pca.fit(trainsetp)
val result_lr_pca = model_lr_pca.transform(testsetp)
val predictionAndLabels_lr_pca = result_lr_pca.select("prediction", "is_churn")

println("Evaluation of Logistic Regression model with dimensionality reduction")
val evaluator_lr_pca = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision").setLabelCol("is_churn")
println("Precision:" + evaluator_lr.evaluate(predictionAndLabels_lr_pca))
val evaluator1_lr_pca = new MulticlassClassificationEvaluator().setMetricName("weightedRecall").setLabelCol("is_churn")
println("Recall:" + evaluator1_lr_pca.evaluate(predictionAndLabels_lr_pca))

//Random Forest Classifier
val rf = new RandomForestClassifier()
  .setLabelCol("is_churn")
  .setFeaturesCol("features")
  .setNumTrees(15)
  .setMaxDepth(4)
//Build pipeline for Random Forest Classifier
val pipeline_rf = new Pipeline()
  .setStages(Array(assembler_train,rf))
//Build Paramgrid Builder for Random Forest Classifier
val paramGrid_rf = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(10,20))
  .addGrid(rf.maxDepth, Array(3,4))
  .build()
//Build evaluator for Random Forest Classifier
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("is_churn")
//Build cross validator for Random Forest Classifier with 5 folds
val cv_rf = new CrossValidator()
  .setEstimator(pipeline_rf)
  .setEstimatorParamMaps(paramGrid_rf)
  .setEvaluator(evaluator)
  .setNumFolds(5)

val model_rf = cv_rf.fit(trainset)
//Apply test data on the model
val result_rf = model_rf.transform(testset)
//Select predication and label columns
val predictionAndLabels_rf = result_rf.select("rawPrediction","prediction", "is_churn")

println("Evaluation of Random Forest model without dimensionality reduction")
val evaluator_rf = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision").setLabelCol("is_churn")
println("Precision:" + evaluator_rf.evaluate(predictionAndLabels_rf))
val evaluator1_rf = new MulticlassClassificationEvaluator().setMetricName("weightedRecall").setLabelCol("is_churn")
println("Recall:" + evaluator1_rf.evaluate(predictionAndLabels_rf))

// SVM
val svm = new LinearSVC()
  .setMaxIter(10)
  .setRegParam(0.1)
  .setLabelCol("is_churn")
  
//Build pipeline for SVM Classifier
val pipeline_svm = new Pipeline()
  .setStages(Array(assembler_train,svm))
//Build Paramgrid Builder for SVM Classifier
 val paramGrid_svm = new ParamGridBuilder()
  .addGrid(svm.threshold, Array(0.1,0.5))
  .addGrid(svm.regParam, Array(0.1,0.001))
  .addGrid(svm.maxIter, Array(10,20))
  .build()
//Build evaluator for SVM Classifier
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("is_churn")
//Build cross validator for SVM Classifier with 5 folds
val cv_svm = new CrossValidator()
  .setEstimator(pipeline_svm)
  .setEstimatorParamMaps(paramGrid_svm)
  .setEvaluator(evaluator)
  .setNumFolds(3)

val model_svm = cv_svm.fit(trainset)
//Apply test data on the model
val result_svm = model_svm.transform(testset)
//Select predication and label columns
val predictionAndLabels_svm = result_svm.select("prediction", "is_churn")

println("Evaluation of SVM model without dimensionality reduction")
val evaluator_svm = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision").setLabelCol("is_churn")
println("Precision:" + evaluator_svm.evaluate(predictionAndLabels_svm))
val evaluator1_svm = new MulticlassClassificationEvaluator().setMetricName("weightedRecall").setLabelCol("is_churn")
println("Recall:" + evaluator1_svm.evaluate(predictionAndLabels_svm))


//Random classifier
val rf_pca = new RandomForestClassifier()
  .setLabelCol("is_churn")
  .setFeaturesCol("features")
  .setNumTrees(15)
  .setMaxDepth(3)
val pipeline_rf_pca = new Pipeline()
  .setStages(Array(rf_pca))
val paramGrid_rf_pca = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(10,20))
  .addGrid(rf.maxDepth, Array(3,4))
  .build()
val evaluator_rf_pca = new BinaryClassificationEvaluator().setLabelCol("is_churn")
val cv_rf_pca = new CrossValidator()
  .setEstimator(pipeline_rf_pca)
  .setEstimatorParamMaps(paramGrid_rf_pca)
  .setEvaluator(evaluator_rf_pca)
  .setNumFolds(2)

val model_rf_pca = cv_rf_pca.fit(trainsetp)
val result_rf_pca = model_rf_pca.transform(testsetp)
val predictionAndLabels_rf_pca = result_rf_pca.select("prediction","rawPrediction", "is_churn")

println("Evaluation of Random Forest model with dimensionality reduction")
val evaluator_rf_pca = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision").setLabelCol("is_churn")
println("Precision:" + evaluator_rf_pca.evaluate(predictionAndLabels_rf_pca))
val evaluator1_rf_pca = new MulticlassClassificationEvaluator().setMetricName("weightedRecall").setLabelCol("is_churn")
println("Recall:" + evaluator1_rf_pca.evaluate(predictionAndLabels_rf_pca))


//Build SVM
val svm_pca = new LinearSVC()
  .setMaxIter(10)
  .setRegParam(0.5)
  .setLabelCol("is_churn")
  
//Build pipeline for Random Forest Classifier
val pipeline_svm_pca = new Pipeline()
  .setStages(Array(svm_pca))
//Build Paramgrid Builder for Random Forest Classifier
 val paramGrid_svm_pca = new ParamGridBuilder()
  .addGrid(svm_pca.threshold, Array(0.1,0.5))
  .addGrid(svm_pca.regParam, Array(0.1,0.001))
  .addGrid(svm_pca.maxIter, Array(10,20))
  .build()
//Build evaluator for Random Forest Classifier
val evaluator_svm_pca = new MulticlassClassificationEvaluator().setLabelCol("is_churn")
//Build cross validator for Random Forest Classifier with 5 folds
val cv_svm_pca = new CrossValidator()
  .setEstimator(pipeline_svm_pca)
  .setEstimatorParamMaps(paramGrid_svm_pca)
  .setEvaluator(evaluator_svm_pca)
  .setNumFolds(3)
  
val model_svm_pca = cv_svm_pca.fit(trainsetp)
//Apply test data on the model
val result_svm_pca = model_svm_pca.transform(testsetp)
//Select predication and label columns
val predictionAndLabels_svm_pca = result_svm_pca.select("prediction", "is_churn")  

println("Evaluation of SVM model with dimensionality reduction")
val evaluator_svm_pca = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision").setLabelCol("is_churn")
println("Precision:" + evaluator_svm_pca.evaluate(predictionAndLabels_svm_pca))
val evaluator1_svm_pca = new MulticlassClassificationEvaluator().setMetricName("weightedRecall").setLabelCol("is_churn")
println("Recall:" + evaluator1_svm_pca.evaluate(predictionAndLabels_svm_pca))
