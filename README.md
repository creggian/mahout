Feature Selection for Mahout
============================

This is a Java library for [Apache Mahout][1] for feature selection algorithms in MapReduce. So far the only algorithm 
available is [mRMR][2] and I'm currently working on [Single Feature Optimization][3].

Content
-------
* *src* contains the source code of the library
* *jar* is the library you can use to run mRMR algorithm
* *example* provides a script with all the commands required to run mRMR

Unit Tests
----------

Tests for the mRMR MapReduce job are not available, since mRMR is implemented taking advantage of the Distributed Cache, while 
MRUnit package does not provide any support for that.

Acknowledgement
---------------

This library has been developed as part of a project supported by [Machine Learning Group][4] and granted by [Spinner][5]

![Alt Spinner](http://mlg.ulb.ac.be/sites/default/files/3_logo_row.png)

[1]: http://mahout.apache.org/
[2]: http://penglab.janelia.org/papersall/docpdf/2005_TPAMI_FeaSel.pdf
[3]: http://www.cs.cmu.edu/~daria/papers/fslr.pdf
[4]: http://mlg.ulb.ac.be/
[5]: http://www.spinner.it/
