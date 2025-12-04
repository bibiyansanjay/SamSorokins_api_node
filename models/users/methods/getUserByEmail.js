import pool from "../../../db";

const getUserByEmail = (email) => {
  return new Promise(async (resolve, reject) => {
    try {
      const data = await pool.query(
        'Select * FROM user WHERE email = ?',
        [email]
      );

      if (data.length === 0) {
        return res.status(404).json({ message: "User not found" });
      }

      const user = data[0];
      //console.log('User found by email:', user);
      //console.log('User Got by email');
      resolve(user);
    } catch (err) {
      reject(err);
    }
  });
};

export default getUserByEmail;