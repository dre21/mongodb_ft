import datetime
import logging

from stemmer import PorterStemmer

class Indexer():

        # remove stop words and do stemming
        
        STOP_WORD_LIST = ["a","a's","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","ain't","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren't","around","as","aside","ask","asking","associated","at","available","away","awfully","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c'mon","c's","came","can","can't","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","course","currently","definitely","described","despite","did","didn't","different","do","does","doesn't","doing","don't","done","down","downwards","during","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","had","hadn't","happens","hardly","has","hasn't","have","haven't","having","he","he's","hello","help","hence","her","here","here's","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i'd","i'll","i'm","i've","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","isn't","it","it'd","it'll","it's","its","itself","just","keep","keeps","kept","know","knows","known","last","lately","later","latter","latterly","least","less","lest","let","let's","like","liked","likely","little","look","looking","looks","ltd","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","que","quite","qv","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","shouldn't","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t's","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that's","thats","the","their","theirs","them","themselves","then","thence","there","there's","thereafter","thereby","therefore","therein","theres","thereupon","these","they","they'd","they'll","they're","they've","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","value","various","very","via","viz","vs","want","wants","was","wasn't","way","we","we'd","we'll","we're","we've","welcome","well","went","were","weren't","what","what's","whatever","when","whence","whenever","where","where's","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","who's","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","won't","wonder","would","would","wouldn't","yes","yet","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","zero"]
        
        def __init__(self):
                logging.debug('Indexer => Init params:self')
                self.idx_fields = []            # field of document to be indexed
                #self.STOP_WORD_LIST = []
                self.P = PorterStemmer()

        # end of function
        '''
        def set_stop_words(self,stop_word_list):
                self.STOP_WORD_LIST = stop_word_list
        # end of function
        '''

        def set_idx_fields(self,fields):
                logging.debug('Indexer => set_idx_fields fields:' + str(fields))
                self.idx_fields = fields
                
        def add_idx_field(self,field_name):
                self.idx_fields.append(field_name)
        
        def clean(self,word):
                #preprocess word
                word = word.lower()
                word = word.strip("\n\t,.(){}?!;'")
                
                if word not in self.STOP_WORD_LIST:
                        word = self.P.stem(word,0,len(word)-1)
                else:
                        word = ""

                return word
        # end of function
        
        def tokenize(self, text):
                #list
                word_idx = []
                        
                # split lines
                lines = text.split('\n')
                
                for line in lines:
                        # split words
                        words = line.split(' ')
                        for word in words:
                                word = self.clean(word)                         
                                if len(word) > 1:
                                        word_idx.append(word)

                # make a set (remove duplicate)
                word_idx = set(word_idx)

                return word_idx
        # end of function
        
        def index(self, document):
                if isinstance(document,list): document = document[0]
                text = ""
                # get text from document to be indexed
                for field in self.idx_fields:
                        text += document[field] + " "
        
                return self.tokenize(text)
        
        def stem(self, words):
                return [self.tokenize(word) for word in words]

        # end of function        
# end of class




if __name__ == '__main__':
        
        # mongodb connection properties
        host = 'localhost'
        port = 27017
        
        # test data
        doc_texts = {}
        doc_texts["MAN KILLED SIX COLLEAGUES 12345"] = "Six people have been shot dead after a Russian lawyer opened fire on his colleagues at a pharmacy company in Moscow.\nThis CCTV footage shows 30-year-old Dmitry Vinogradov entering the building with a tall bag on his back.\nHe walks along the corridor and out of shot and then returns dressed in camouflage gear carrying two riffles.\nA cleaning woman comes running out after he shot four men and two women at their desks.\nVinogradov eventually gave himself up to security who dragged him to the ground.\nThe Russian investigative committee said there appeared to be a drunken rampage after his ex-girlfriend, a pharmacist at the company, left him."
        doc_texts["VENICE IS UNDER WATER "] = "Water and Venice usually go together like bees and honey. But not when there's as much rain as there's been over the past few days.\nSeventy percent of Italy's famous lagoon city's been flooded and these bizarre pictures of a Concordia cruise ship sailing past provided a spectacle for tourists.\nPeople have been filmed wading through waist-deep water while cafes and shops have had to close. These tables in St Mark's Square almost completely submerged.\nThis is only the sixth flood of this magnitude in Venice since 1872 and with more rain expected, the weather forecast for the rest of the week doesn't look promising."
        doc_texts["TRAIN HITS A BIG CAR"] = "Two men inside the utility truck have a lucky escape after a passing freight train collides with their vehicle in Utah.\nThe dramatic video filmed by an onlooker on his phone shows the train passing through Wellington with his horn blowing shortly before the crash.\nMangled remains were all that was left of the truck which miraculously the passengers managed to climb around off unscathed. \nThe vehicle's driver has since told police he had not seen the oncoming train."
        doc_texts["BIG STORM DESTROYS NEW YORK"] = "Super storm Sandy gives New York a historic drenching.\nBattery Park in lower Manhattan floods as record high water levels above 13 feet hit the city.\nStreets nearby are under water and many subways have been flooded.\nSandy has now hit the US east coast and Canada leaving at least thirteen dead."
        author = "http://www.newsinlevels.com"
        
        
        # connection to mongoDB
        #con = Connection(host,port)
        #db = con.myblog
        # define posts collection & inverted index collection
        #posts = db.posts
        #invidx = db.invidx
        
        # initialize indexer
        Idx = Indexer()
        #Idx.set_stop_words(stop_words)
        Idx.set_idx_fields(["title","content"])
        
        '''
        for title,content in doc_texts.iteritems():
                post = {"author": author, "title": title, "content": content, "time": datetime.datetime.utcnow()}
                print post
        
        '''
        # create 1 post
        title = "MAN KILLED SIX COLLEAGUES 12345"
        post = {"author": author, "title": title, "content": doc_texts[title], "time": datetime.datetime.utcnow()}
        words =  Idx.index(post)
        

        #words = Idx.stem(["studies","studying", "student"])
        # inserting post to posts collection
        #obj_id = posts.insert(post)
        #if obj_id != None: print "inserting post OK"
                        
        
        for word in words:
                # looping for updating inverted index table
                #invidx.update({"word":word},{"$push":{"docs":obj_id}},True)
                print "Now updating inverted index for", word
        #print obj_id



