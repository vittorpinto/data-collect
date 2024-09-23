#%%
name = 'joao pinto'
name = '.'.join(w[0] for w in name.split(' ')).upper()
name
# %%
numbers = 0
def digitize(numbers):
    return [num.split() for num in format(numbers)[::-1]]
print(digitize(numbers))
# %%
sentence = "is2 Thi1s T4est 3a"
splitted_sentence = sentence.split(' ')
len_sentence = len(splitted_sentence)
new_sentence = [0 for x in range(0,len_sentence)]
new_sentence
#%%
for word in splitted_sentence:
    for letter in word:
        if letter.isnumeric():
            new_sentence[int(letter)-1] = word
# %%
name = 'raqh'
vogals = 'aeiou'
# %%
def is_alt(string):
    arr = []
    tempI = 0
    count = 0
    
    for letter in string:
        count+=1
        for vogal in vogals:
            tempI = letter.find(vogal)
            if tempI != -1: 
                arr.append(count)

    print (arr)
    
    if len(arr) > 0:
        for i in range(len(arr)-1):
            if arr[i+1] - arr[i] != 2:
                return False
            else:
                pass
    else:
        return False
        
    return True
# %%
is_alt(name)

# %%
