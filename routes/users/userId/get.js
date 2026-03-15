import { User } from "../../../models";

export default async (req, res, next) => {
  try {
    const { userId } = req;

    if (!userId) {
      return res.status(404).json({ message: "User Not Exist." });
    }

    const user = await User.findOne({ _id: userId, isDeleted: false });

    // This used for modifying the fields for notifications
    // .lean();

    // Fetch unread notifications count for the user

    // const announcementCount = await Announcement.countDocuments({
    //   targetAudience: { $in: [user?.role] },
    //   read: { $nin: [user?._id] },
    //   isDeleted: false,
    //   isDraft: false,
    // });
    // user.unreadNotification = announcementCount;
    return res.json({ message: "User Fetched Successfully", user });
  } catch (error) {
    next(error);
  }
};
