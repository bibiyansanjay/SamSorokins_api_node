import { Router } from "express";
import questions from "./questions";
import packs from "./packs";
import category from "./category";
import kpi from "./kpi";
import stakeholder from "./stakeholder";
import segment from "./segment";
import templates from "./templates";
import questionsNew from "./questionsNew";
import respondents from "./respondents";
//
import onlineRespondents from "./onlineRespondents";
import onlineQuestions from "./onlineQuestions";

const router = Router();

router.get("/questions", questions);
router.get("/questionsNew", questionsNew);
router.get("/packs", packs);
router.get("/category", category);
router.get("/kpi", kpi);
router.get("/stakeholder", stakeholder);
router.get("/segment", segment);
router.get("/templates", templates);
router.get("/respondents", respondents);
//
router.post("/online/respondents", onlineRespondents); //csv upload by client
router.post("/online/questions", onlineQuestions); //csv upload by client

export default router;
