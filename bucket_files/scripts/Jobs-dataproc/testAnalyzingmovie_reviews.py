import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SQLContext
from pyspark import SparkContext


spark = sparknlp.start()
#spark.executor.memory
MODEL_NAME='sentimentdl_use_imdb'
text_list = []
if MODEL_NAME=='sentimentdl_use_imdb':
  text_list = [
             """Demonicus is a movie turned into a video game! I just love the story and the things that goes on in the film.It is a B-film ofcourse but that doesn`t bother one bit because its made just right and the music was rad! Horror and sword fight freaks,buy this movie now!""",
             """Back when Alec Baldwin and Kim Basinger were a mercurial, hot-tempered, high-powered Hollywood couple they filmed this (nearly) scene-for-scene remake of the 1972 Steve McQueen-Ali MacGraw action-thriller about a fugitive twosome. It almost worked the first time because McQueen was such a vital presence on the screen--even stone silent and weary, you could sense his clock ticking, his cagey magnetism. Baldwin is not in Steve McQueen's league, but he has his charms and is probably a more versatile actor--if so, this is not a showcase for his attributes. Basinger does well and certainly looks good, but James Woods is artificially hammy in a silly mob-magnet role. A sub-plot involving another couple taken hostage by Baldwin's ex-partner was unbearable in the '72 film and plays even worse here. As for the action scenes, they're pretty old hat, which causes one to wonder: why even remake the original?""",
             """Despite a tight narrative, Johnnie To's Election feels at times like it was once a longer picture, with many characters and plot strands abandoned or ultimately unresolved. Some of these are dealt with in the truly excellent and far superior sequel, Election 2: Harmony is a Virtue, but it's still a dependably enthralling thriller about a contested Triad election that bypasses the usual shootouts and explosions (though not the violence) in favour of constantly shifting alliances that can turn in the time it takes to make a phone call. It's also a film where the most ruthless character isn't always the most threatening one, as the chilling ending makes only too clear: one can imagine a lifetime of psychological counselling being necessary for all the trauma that one inflicts on one unfortunate bystander. Simon Yam, all too often a variable actor but always at his best under To's direction, has possibly never been better in the lead, not least because Tony Leung's much more extrovert performance makes his stillness more the powerful.""",
             """This movie has successfully proved what we all already know, that professional basket-ball players suck at everything besides playing basket-ball. Especially rapping and acting. I can not even begin to describe how bad this movie truly is. First of all, is it just me, or is that the ugliest kid you have ever seen? I mean, his teeth could be used as a can-opener. Secondly, why would a genie want to pursue a career in the music industry when, even though he has magical powers, he sucks horribly at making music? Third, I have read the Bible. In no way shape or form did it say that Jesus made genies. Fourth, what was the deal with all the crappy special effects? I assure you that any acne-addled nerdy teenager with a computer could make better effects than that. Fifth, why did the ending suck so badly? And what the hell is a djin? And finally, whoever created the nightmare known as Kazaam needs to be thrown off of a plane and onto the Eiffel Tower, because this movie take the word "suck" to an entirely new level.""",
             """The fluttering of butterfly wings in the Atlantic can unleash a hurricane in the Pacific. According to this theory (somehow related to the Chaos Theory, I'm not sure exactly how), every action, no matter how small or insignificant, will start a chain reaction that can lead to big events. This small jewel of a film shows us a series of seemingly-unrelated characters, most of them in Paris, whose actions will affect each others' lives. (The six-degrees-of-separation theory can be applied as well.) Each story is a facet of the jewel that is this film. The acting is finely-tuned and nuanced (Audrey Tautou is luminous), the stories mesh plausibly, the humor is just right, and the viewer leaves the theatre nodding in agreement.""",
             """There have been very few films I have not been able to sit through. I made it through Battle Field Earth no problem. But this, This is one of the single worst films EVER to be made. I understand Whoopi Goldberg tried to get of acting in it. I do not blame her. I would feel ashamed to have this on a resume. I belive it is a rare occasion when almost every gag in a film falls flat on it's face. Well it happens here. Not to mention the SFX, look for the dino with the control cables hanging out of it rear end!!!!!! Halfway through the film I was still looking for a plot. I never found one. Save yourself the trouble of renting this and save 90 minutes of your life.""",
             """After a long hard week behind the desk making all those dam serious decisions this movie is a great way to relax. Like Wells and the original radio broadcast this movie will take you away to a land of alien humor and sci-fi paraday. 'Captain Zippo died in the great charge of the Buick. He was a brave man.' The Jack Nicholson impressions shine right through that alien face with the dark sun glasses and leather jacket. And always remember to beware of the 'doughnut of death!' Keep in mind the number one rule of this movie - suspension of disbelief - sit back and relax - and 'Prepare to die Earth Scum!' You just have to see it for yourself.""",
             """When Ritchie first burst on to movie scene his films were hailed as funny, witty, well directed and original. If one could compare the hype he had generated with his first two attempts and the almost universal loathing his last two outings have created one should consider - has Ritchie been found out? Is he really that talented? Does he really have any genuine original ideas? Or is he simply a pretentious and egotistical director who really wants to be Fincher, Tarantino and Leone all rolled into one colossal and disorganised heap? After watching Revolver one could be excused for thinking were did it all go wrong? What happened to his great sense of humour? Where did he get all these mixed and convoluted ideas from? Revolver tries to be clever, philosophical and succinct, it tries to be an intelligent psychoanalysis, it tries to be an intricate and complicated thriller. Ritchie does make a gargantuan effort to fulfil all these many objectives and invests great chunks of a script into existential musings and numerous plot twists. However, in the end all it serves is to construct a severely disjointed, unstructured and ultimately unfriendly film to the audience. Its plagiarism is so sinful and blatant that although Ritchie does at least attempt to give his own spin he should be punished for even trying to pass it off as his own work. So what the audience gets ultimately is a terrible screenplay intertwined with many pretentious oneliners and clumsy setpieces.<br /><br />Revolver is ultimately an unoriginal and bland movie that has stolen countless themes from masterpieces like Fight Club, Usual Suspects and Pulp Fiction. It aims high, but inevitably shots blanks aplenty.<br /><br />Revolver deserves to be lambasted, it is a truly poor film masquerading as a wannabe masterpiece from a wannabe auteur. However, it falls flat on its farcical face and just fails at everything it wants to be and achieve.""",
             """I always thought this would be a long and boring Talking-Heads flick full of static interior takes, dude, I was wrong. "Election" is a highly fascinating and thoroughly captivating thriller-drama, taking a deep and realistic view behind the origins of Triads-Rituals. Characters are constantly on the move, and although as a viewer you kinda always remain an outsider, it\'s still possible to feel the suspense coming from certain decisions and ambitions of the characters. Furthermore Johnnie To succeeds in creating some truly opulent images due to meticulously composed lighting and atmospheric light-shadow contrasts. Although there\'s hardly any action, the ending is still shocking in it\'s ruthless depicting of brutality. Cool movie that deserves more attention, and I came to like the minimalistic acoustic guitar score quite a bit.""",
             """This is to the Zatoichi movies as the "Star Trek" movies were to "Star Trek"--except that in this case every one of the originals was more entertaining and interesting than this big, shiny re-do, and also better made, if substance is more important than surface. Had I never seen them, I would have thought this good-looking but empty; since I had, I thought its style inappropriate and its content insufficient. The idea of reviving the character in a bigger, slicker production must have sounded good, but there was no point in it, other than the hope of making money; it\'s just a show, which mostly fails to capture the atmosphere of the character\'s world and wholly fails to take the character anywhere he hasn\'t been already (also, the actor wasn\'t at his best). I\'d been hoping to see Ichi at a late stage of life, in a story that would see him out gracefully and draw some conclusion from his experience overall; this just rehashes bits and pieces from the other movies, seasoned with more sex and sfx violence. Not the same experience at all."""
             ]
elif  MODEL_NAME=='sentimentdl_use_twitter':
  text_list = [
            """@Mbjthegreat i really dont want AT&amp;T phone service..they suck when it comes to having a signal""",
            """holy crap. I take a nap for 4 hours and Pitchfork blows up my twitter dashboard. I wish I was at Coachella.""",
            """@Susy412 he is working today  ive tried that still not working..... hmmmm!! im rubbish with computers haha!""",
            """Brand New Canon EOS 50D 15MP DSLR Camera Canon 17-85mm IS Lens ...: Web Technology Thread, Brand New Canon EOS 5.. http://u.mavrev.com/5a3t""",
            """Watching a programme about the life of Hitler, its only enhancing my geekiness of history.""",
            """GM says expects announcment on sale of Hummer soon - Reuters: WDSUGM says expects announcment on sale of Hummer .. http://bit.ly/4E1Fv""",
            """@accannis @edog1203 Great Stanford course. Thanks for making it available to the public! Really helpful and informative for starting off!""",
            """@the_real_usher LeBron is cool.  I like his personality...he has good character.""",
            """@sketchbug Lebron is a hometown hero to me, lol I love the Lakers but let's go Cavs, lol""",
            """@PDubyaD right!!! LOL we'll get there!! I have high expectations, Warren Buffet style.""",
            ]            
#Paths to data
raw_csv_path="gs://debootcamptest/raw-data/movie_review.csv"
stagging_path="gs://debootcamptest/stagging-data/movie_sentiment"

#definition of nlp pipeline
documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document")
databypass=DocumentAssembler().setInputCol("cid").setOutputCol("cid")
use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
 .setInputCols(["document"])\
 .setOutputCol("sentence_embeddings")


sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en")\
    .setInputCols(["sentence_embeddings"])\
    .setOutputCol("sentiment")

nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          databypass,
          use,
          sentimentdl
      ])

empty_df = spark.createDataFrame([['']]).toDF("text")

pipelineModel = nlpPipeline.fit(empty_df)


#df = spark.createDataFrame(pd.DataFrame({"text":text_list}))
raw_dataframe=spark.read.csv(raw_csv_path,
                             header=True,
                             sep=',',
                             inferSchema=True)
raw_dataframe_text=raw_dataframe.withColumnRenamed("review_str", "text")

#Apply model
result = pipelineModel.transform(raw_dataframe_text)

#Create new dataframe to generate the parquet to save in stagging area
proc_dataframe=result.select(F.explode(F.arrays_zip('cid.result','document.result', 'sentiment.result')).alias("cols")) \
              .select(F.expr("cols['0']").alias('id'),F.expr("cols['2']").alias('sentiment'))

#Succesfull instruction to save parquet into gs
proc_dataframe.coalesce(1).write.option("header","true").mode('overwrite').parquet(stagging_path)

 #save to parquet (not working)

#proc_dataframe.cache()
#proc_dataframe.write.parquet(stagging_path)

#instructions that do not work to save the data
#proc_dataframe.write.option("header",True) \
#        .option("maxRecordsPerFile", 200) \
#        .mode("overwrite") \
#        .csv(stagging_path)

#refactor
# result_factor=  result_factor.withColumn("columnindex", row_number().over(w))
# result_factor.show(5)
# raw_dataframe_text=  raw_dataframe_text.withColumn("columnindex", row_number().over(w))
# raw_dataframe_text.show(5)
# end_dataframe= raw_dataframe_text.join(result_factor, raw_dataframe_text.columnindex == result_factor.columnindex, 'inner').drop(result_factor.columnindex)

# #end_dataframe.show()



# #Posible refactor change pos to 1 and neg to 0
# #result.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")) \
# #.select(F.expr("cols['0']").alias("document"),
# #        F.expr("cols['1']").alias("sentiment")).show(truncate=False)