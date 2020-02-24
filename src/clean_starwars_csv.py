import pandas as pd

swdf = pd.read_table(
    "../input/raw/SW_EpisodeIV.txt", delim_whitespace=True, header=0, escapechar="\\"
)

user_name = [i.capitalize() for i in swdf.character]

swdf["user_name"] = user_name
swdf = swdf.rename(columns={"dialogue": "quote"})
swdf = swdf[["user_name", "quote"]]

swdf.to_csv("../input/starwars.csv")

