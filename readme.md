# Test pour le poste de Data Engineer @ MyTraffic

Ceci est ma réponse au test donné par MyTraffic !
La réponse est codée en python et comporte deux composantes: dans le dir `src`
on a les fonctions qui transforment les données, et dans le dir `tst` on charge les données
et on appelle les routines de `src`.

J'ai commençé par coder en python une solution avec une librairie familière: pandas.
Dans un premier temps je réponds aux 4 questions avec pandas, puis dans un second temps je reproduis
le même résultat avec PySpark.

Pour installer les dépendences si nécessaire, faire

```
$ pip install -r requirements.txt
```

# Solution Pandas

Les réponses aux questions 1,2,3 et 4 sont disponibles.
Pour obtenir un aperçu de la réponse, il suffit de lancer le script correspondant
depuis la racine du projet: par exemple pour la question 1 cela donne:

```
$ python3 tst/pandas/pandas_first_question_answer.py
```

# Solution PySpark

Les réponses aux questions 1,2,3 et 4 sont disponibles.
Pour obtenir un aperçu de la réponse, il suffit de lancer le script correspondant
depuis la racine du projet: par exemple pour la question 1 cela donne:

```
$ python3 tst/spark/spark_first_question_answer.py
```
