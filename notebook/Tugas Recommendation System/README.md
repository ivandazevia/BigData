# Tugas Implementasi Recommendation Systems

**_server.py_** : menginisialisasi server web CherryPy yang menjalankan Flask _app.py_ untuk membuat konteks _engine.py_ berbasis Spark. <br>
**_engine.py_** : inti dari sistem dan menyimpan semua perhitungan yang ada. <br>
**_app.py_** : penghubung _server.py_ dan _engine.py_, sebagai tempat routing.

## REST API
> `Top Ratings` : **GET** /<int:user_id>/ratings/top/<int:count> <br>
> `Movie Ratings` : **GET** /<int:user_id>/ratings/<int:movie_id> <br>
> `Ratings History` : **GET** /<int:user_id>/history <br>
> `Movie Recommend` : **GET** /movies/<int:movie_id>/recommend/<int:count> <br>


### Top Ratings 
![img1](img/TR.jpg)

### Movie Ratings
![img2](img/MR.jpg)

### Ratings History
![img3](img/RH.jpg)

### Movie Recommend
![img4](img/MRec.jpg)
