pred_data = spark.createDataFrame(
[(5.1, 3.5, 1.4, 0.2),
(6.2, 3.4, 5.4, 2.3)],
["sepal_length", "sepal_width", "petal_length", "petal_width"])


predictions = logreg.transform(pred_data)

predictions.show()

# Extract the predicted class labels as integers
predicted_classes = predictions.select("prediction").rdd.flatMap(lambda x: x).collect()
predicted_classes = [int(x) for x in predicted_classes]

# Write the predicted class labels to a CSV file in the desired format
with open("out/out_3_2.txt", "w") as f:
    f.write("class\n")
    for c in predicted_classes:
        f.write(f"<class {c}>\n")

