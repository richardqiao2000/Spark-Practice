# Data Description

## Data format:
  <postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
  1,27233496,,,0,C#
  1,23698767,,,9,C#
  1,5484340,,,0,C#
  2,5494879,,5484340,1,
  1,9419744,,,2,Objective-C
  1,26875732,,,1,C#
  1,9002525,,,2,C++
  2,9003401,,9002525,4,
  2,9003942,,9002525,1,
  2,9005311,,9002525,0,
  
## Column description  
* <postTypeId>:     Type of the post. Type 1 = question, 
                  type 2 = answer.
                  
* <id>:             Unique id of the post (regardless of type).

* <acceptedAnswer>: Id of the accepted answer post. This
                  information is optional, so maybe be missing 
                  indicated by an empty string.
                  
* <parentId>:       For an answer: id of the corresponding 
                  question. For a question:missing, indicated
                  by an empty string.
                  
* <score>:          The StackOverflow score (based on user 
                  votes).
                  
* <tag>:            The tag indicates the programming language 
                  that the post is about, in case it's a 
                  question, or missing in case it's an answer.