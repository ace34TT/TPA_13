convertDFForPrediction <- function(marketing_df) {
    marketing_df$id <- NULL
    marketing_df$sexe <- factor(marketing_df$sexe, levels = c("F", "M"))
    levels(marketing_df$sexe) <- c("F", "H")
    levels_situationfamiliale <- c("Célibataire", "En Couple", "Seul(e)", "Divorcé(e)")
    marketing_df$situationfamiliale <- factor(marketing_df$situationfamiliale,
        levels = levels_situationfamiliale
    )
    marketing_df$deuxiemevoiture <- factor(marketing_df$deuxiemevoiture,
        levels = c("true", "false")
    )
    return(marketing_df)
}

convertPredictionToDBSchema <- function(marketing_df, single_prediction) {
    marketing_df$categorie <- single_prediction
    marketing_df$deuxiemevoiture <- as.integer(marketing_df$deuxiemevoiture)
    marketing_df$categorie <- as.character(marketing_df$categorie)
    marketing_df$situationfamiliale <- as.character(marketing_df$situationfamiliale)
    marketing_df$deuxiemevoiture <- as.numeric(marketing_df$deuxiemevoiture) - 1
    return(marketing_df)
}
