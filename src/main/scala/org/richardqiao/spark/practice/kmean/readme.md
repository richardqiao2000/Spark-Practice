# Data Description

## Data format:
\<postTypeId>,\<id>,[\<acceptedAnswer>],[\<parentId>],\<score>,[\<tag>]
- 1,27233496,,,0,C#
- 1,23698767,,,9,C#
- 1,5484340,,,0,C#
- 2,5494879,,5484340,1,
- 1,9419744,,,2,Objective-C
- 1,26875732,,,1,C#
- 1,9002525,,,2,C++

## Column description  
* \<postTypeId>:     Type of the post. Type 1 = question, 
                  type 2 = answer.
                  
* \<id>:             Unique id of the post (regardless of type).

* \<acceptedAnswer>: Id of the accepted answer post. This
                  information is optional, so maybe be missing 
                  indicated by an empty string.
                  
* \<parentId>:       For an answer: id of the corresponding 
                  question. For a question:missing, indicated
                  by an empty string.
                  
* \<score>:          The StackOverflow score (based on user 
                  votes).
                  
* \<tag>:            The tag indicates the programming language 
                  that the post is about, in case it's a 
                  question, or missing in case it's an answer.
                  
## Goal:
* The overall goal  is to implement a distributed k-means algorithm which clusters posts on StackOverflow according to their score. Moreover, this clustering should be executed in parallel for different programming languages, and the results should be compared.

* The motivation is as follows: On StackOverflow, different user-provided answers may have very different ratings (based on user votes) based on their perceived value. Therefore, we would like to look at the distribution of questions and their answers. For example, how many highly-rated answers do StackOverflow users post, and how high are their scores? Are there big differences between higher-rated answers and lower-rated ones?

* Finally, we are interested in comparing these distributions for different programming language communities. Differences in distributions could reflect differences in the availability of documentation. For example, StackOverflow could have better documentation for a certain library than that library's API documentation. However, to avoid invalid conclusions we will focus on the well-defined problem of clustering answers according to their scores.

