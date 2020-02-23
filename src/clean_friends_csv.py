import pandas as pd
import warnings

warnings.filterwarnings("ignore")

df = pd.read_csv("../input/raw/friends_quotes.csv")

# Clean NAs
df = df.dropna(subset=["author", "episode_number", "quote", "quote_order", "season"])

# Unify main results
df = df.replace(
    {
        "ROSS": "Ross",
        "CHANDLER": "Chandler",
        "Chandle": "Chandler",
        "CHAN": "Chandler",
        "MONICA": "Monica",
        "MNCA": "Monica",
        "JOEY": "Joey",
        "PHOEBE": "Phoebe",
        "PHOE": "Phoebe",
        "Phoeb": "Phoebe",
        "RACHEL": "Rachel",
        "RACH": "Rachel",
        "Rache": "Rachel",
    }
)

# Filter main characters
friends = ["Rachel", "Monica", "Phoebe", "Ross", "Chandler", "Joey"]
dfriends = df[df.author.isin(friends)]

# Cast the float values
lst = ["episode_number", "quote_order", "season"]
for i in lst:
    dfriends.loc[:, i] = dfriends[i].astype(int)

# Change name
dfriends = dfriends.rename(columns={"Unnamed: 0": "msg_id", "author": "user_name"})

# Create a column to register the time order of appearence of each character.
intern_ord = []
c_rach, c_mon, c_phoe, c_ross, c_chand, c_joey = -1, -1, -1, -1, -1, -1
for i in dfriends.user_name:
    if i == "Rachel":
        c_rach += 1
        intern_ord.append(c_rach)
    elif i == "Monica":
        c_mon += 1
        intern_ord.append(c_mon)
    elif i == "Phoebe":
        c_phoe += 1
        intern_ord.append(c_phoe)
    elif i == "Ross":
        c_ross += 1
        intern_ord.append(c_ross)
    elif i == "Chandler":
        c_chand += 1
        intern_ord.append(c_chand)
    elif i == "Joey":
        c_joey += 1
        intern_ord.append(c_joey)

# Create new column
dfriends["user_order"] = intern_ord

# Reorder columns
dfriends = dfriends[
    [
        "msg_id",
        "user_name",
        "user_order",
        "season",
        "episode_number",
        "episode_title",
        "quote_order",
        "quote",
    ]
]

# Create clean csv
dfriends.to_csv("../input/friends.csv")

# Also devide by season and episode to test the app.
dfs1 = df[df.season == 1]
dfs1e1 = dfs1[dfs1.episode_number == 1]
dfs1.to_csv("../input/friends_s1.csv")
dfs1e1.to_csv("../input/friends_s1_e1.csv")

# Print result
print("\n------------------------------")
print("CSV Files created succesfully")
print("- shape --------\n", dfriends.shape)
print("- authors ------\n", dfriends.user_name.value_counts())
print("- head ---------\n", dfriends.head())

