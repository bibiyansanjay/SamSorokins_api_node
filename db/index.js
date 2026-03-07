import mongoose from "mongoose";

/**
 * @module DataBase
 * @description DataBase related methods define here.
 */

/**
 * @method connection_DB
 * @description Establishes a connection to the database.
 */

// const mongoURI ="mongodb+srv://sanjay:BloCAzBJIvza7rpl@cluster0.giccg.mongodb.net/";

export default async () => {
  try {
    await mongoose.connect(process.env.URI, {
      dbName: process.env.DB_NAME,
    });
    console.log("Database connected....");
  } catch (error) {
    console.log(error);
  }
};
