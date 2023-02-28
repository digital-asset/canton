// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import org.scalatest.wordspec.AsyncWordSpec

trait ServiceAgreementStoreTest { this: AsyncWordSpec with BaseTest =>
  val domain1 = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain1::test"))
  val domain2 = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain2::test"))
  val agreement1 = ServiceAgreementId.tryCreate("agreement1")
  val agreement2 = ServiceAgreementId.tryCreate("agreement2")
  val largeAgreementText =
    String256M.tryCreate("""This agreement (the "AGREEMENT"), entered into between DA and You as of
                         |the date You indicated your acceptance to the terms of this Agreement
                         |(the "EFFECTIVE DATE"), governs Your access to and use of the Canton
                         |Service, described below. "DA" means either Digital Asset Holdings, LLC,
                         |a Delaware Limited Liability Company with offices at 4 World Trade
                         |Center, 150 Greenwich St., 47th Floor, New York, NY 10007, if Your
                         |billing address is located in the United States or Digital Asset
                         |(Switzerland) GmbH, a Swiss company with limited liability, having
                         |offices at Thurgauerstrasse 40, 8050 Zurich, Switzerland, if Your
                         |billing address is outside of the United States. "YOU" or "YOUR" means
                         |the individual or legal entity agreeing to the terms of this Agreement.
                         |Each party may be referred to herein individually as a "PARTY" and
                         |collectively the "PARTIES".
                         |
                         |By entering into this Agreement, the Parties agree as follows:
                         |
                         |1.  PRODUCTS AND SERVICES
                         |
                         |    1.  _COVERED PRODUCTS AND SERVICES_. This Agreement covers Your use
                         |        of a pre-alpha version of the Canton Service, as defined below.
                         |
                         |    2.  _ACCESS TO CANTON SERVICE_. Subject to the terms of this
                         |        Agreement, DA will provide You with authorized access to use the
                         |        Canton Service in the Territory. For purposes of this Agreement,
                         |        the "CANTON SERVICE" means a pre-alpha version of any service DA
                         |        makes available to you that uses Canton Software so that You can
                         |        submit transactions or otherwise participate in a Canton domain
                         |        or network, the "CANTON SOFTWARE" means any goods, licensed
                         |        materials, and software made available by DA that relates to its
                         |        ledger technology, and the "Territory" means worldwide except
                         |        for Australia and New Zealand.
                         |
                         |    3.  _AUTHORIZED USERS_. You will ensure that: (a) if You are an
                         |        individual, only You, and (b) if You are entering into this
                         |        Agreement on behalf of a company or other legal entity, only
                         |        that company or entity's employees, its Affiliates and the
                         |        employees of its Affiliates who have a need to access the Canton
                         |        Software and/or Service (in each case, the "AUTHORIZED USERS")
                         |        have access to the Canton Service.  You shall be liable for any
                         |        breach of any term of this Agreement by any of your Authorized
                         |        Users.  "AFFILIATE" means, with respect to any entity, any other
                         |        entity that, directly or indirectly, through one or more
                         |        intermediaries, controls, is controlled by, or is under common
                         |        control with, such entity. The term "control" means the
                         |        possession, directly or indirectly, of the power to direct or
                         |        cause the direction of the management and policies of an entity,
                         |        whether through the ownership of voting securities, by contract,
                         |        or otherwise.
                         |
                         |    4.  _YOUR DATA AND APPLICATIONS_. You may only use mock or fake data
                         |        with the Canton Service (_e.g._, to develop custom applications,
                         |        referred to herein as "CUSTOM APPLICATIONS"; and collectively
                         |        with such mock data, "YOUR DATA"). You shall not use any
                         |        real-world data with the Canton Service, including but not
                         |        limited to data pertaining to real world transactions or events,
                         |        persons, entities, or subjects, financial data, or Personal
                         |        Information or Sensitive Data. As between You and DA, You retain
                         |        ownership of the Your Data, subject to DA's ownership of the
                         |        Canton Service (including Daml libraries). You hereby grant DA
                         |        the right to use and access Your Data in connection with the
                         |        provision of any services, provided that DA does not (a)
                         |        disclose Your Data to a third party, except as compelled by law
                         |        in accordance with this Agreement, as expressly permitted in
                         |        writing by You, or as required for the provision of the Canton
                         |        Service; or (b) access, use, or modify Your Data, except as
                         |        required to provide the Canton Service, to prevent or address
                         |        service or technical problems, at Your request (_e.g._, in
                         |        connection with customer support matters), or as required for
                         |        the protection of DA's rights and interests. You shall be solely
                         |        responsible and liable with respect to Your Data.
                         |
                         |    5.  _PERSONAL INFORMATION OR SENSITIVE DATA_. Unless You enter into
                         |        a separate Data Processing Agreement with DA, and except as
                         |        provided in Section 10.2, You shall not disclose, enter, load,
                         |        use, or otherwise exploit any Personal Information or Sensitive
                         |        Data in connection with the Canton Service, including but not
                         |        limited to in connection with Your development or use of Your
                         |        Custom Applications, and shall not otherwise provide or furnish
                         |        any Personal Information or Sensitive Data to DA or any of its
                         |        employees, directors, officers, representatives, contractors, or
                         |        agents. DA does not intend uses of the Canton Service to create
                         |        obligations under the Health Insurance Portability and
                         |        Accountability Act of 1996 (as it may be amended from time to
                         |        time and its implementing regulations, "HIPAA"), and makes no
                         |        representations that any Service satisfies HIPAA requirements.
                         |        If You are (or become) a Covered Entity or Business Associate,
                         |        as defined in HIPAA, You will not use the Canton Service for any
                         |        purpose or in any manner involving Protected Health Information
                         |        (as defined in HIPAA) unless specifically permitted pursuant to
                         |        a separate Data Processing Agreement with DA. "PERSONAL
                         |        INFORMATION OR SENSITIVE DATA" means any information relating to
                         |        an identified or identifiable natural person, including any and
                         |        all similarly protected sensitive data; an identifiable natural
                         |        person is one who can be identified, directly or indirectly, in
                         |        particular by reference to an identifier such as a name, an
                         |        identification number, location data, an online identifier or to
                         |        one or more factors specific to the physical, physiological,
                         |        genetic, mental, economic, cultural or social identity of that
                         |        natural person. The term Your Data does not include Personal
                         |        Information or Sensitive Data.
                         |
                         |    6.  _LOSS OF YOUR DATA_. In the event of any breach that compromises
                         |        the security, confidentiality, or integrity of Your Data, DA
                         |        shall notify You as soon as reasonably practicable after
                         |        becoming aware of such occurrence. If the breach compromises the
                         |        security, confidentiality, or integrity of Personal Information
                         |        or Sensitive Data, notwithstanding the fact that such data is
                         |        not permitted for use with the Canton Service, You are solely
                         |        responsible and shall be obligated to: (i) notify the affected
                         |        individuals as soon as practicable but no later than is required
                         |        to comply with applicable law, and (ii) perform or take any
                         |        other actions required to comply with applicable law as a result
                         |        of the occurrence. You agree that it is Your responsibility to
                         |        back up Your Data and any Custom Applications separately away
                         |        from the Canton Service. You agree that DA has no responsibility
                         |        to back up Your Data or any Custom Application and that DA bears
                         |        no liability for any loss of Your Data or any Custom
                         |        Application.
                         |
                         |        Notwithstanding the terms of any non-disclosure or confidentiality
                         |        agreement, if any, that may bind the Parties, the disclosure of Your
                         |        Data due to the act of a third party and the loss of Your Data are
                         |        addressed solely in this Section and are not covered by any other
                         |        non-disclosure or confidentiality agreement. In such event, DA's sole
                         |        responsibility is to act in accordance with this Section, and DA will
                         |        not be liable to You or any other party so long as it meets its
                         |        obligations in this Section.
                         |
                         |    7.  _EXCESSIVE RESOURCE USE_. If DA determines in good faith that Your
                         |        resource usage (including, but not limited to, bandwidth, processing
                         |        cycles, storage, etc.) of the Canton Service is significantly
                         |        excessive in relation to DA's policies, DA reserves the right to
                         |        suspend Your use of the Canton Service or throttle Your access to
                         |        the Canton Service until You reduce such excessive resource
                         |        consumption.
                         |
                         |    8.  _SUSPENSION OR TERMINATION OF THE SERVICE_. DA may suspend,
                         |        terminate, or restrict Your access to the Canton Service under any
                         |        of the following circumstances, and in such event DA will provide
                         |        You with reasonably prompt notice of the reasons for such suspension
                         |        or termination: (a) DA determines in good faith that You have
                         |        violated any of the restrictions set forth in this Agreement,
                         |        violated DA's Acceptable Use Policy available below that DA may
                         |        update from time to time (the "ACCEPTABLE USE POLICY"), or used the
                         |        Canton Service for any purpose other than the Purpose; or (b) DA
                         |        determines in good faith that such suspension is advisable (i) for
                         |        security reasons, (ii) in response to a request from law enforcement
                         |        or a governing body or in relation to legal proceedings, (iii) to
                         |        protect DA from liability, (iv) for the continued normal and
                         |        efficient operation of the Canton Service or to permit DA to
                         |        maintain or update the Canton Service. If You become aware that any
                         |        portion of Your Data or any activity violates this Agreement or any
                         |        applicable law, You shall immediately take all necessary action to
                         |        cease and prevent such activity and remove the portion of Your Data
                         |        violating this Agreement or applicable law from the Canton Service.
                         |        To the extent DA becomes aware that any part of Your Data, in DA's
                         |        reasonable discretion, is in violation of this Agreement or any
                         |        applicable law, DA may immediately delete or remove that part of
                         |        Your Data from the Canton Service or take any other action DA deems
                         |        appropriate. DA reserves the right to cooperate with any legal
                         |        authority and relevant third-party in the investigation of alleged
                         |        wrongdoing. Any violation of applicable law by You shall be Your
                         |        sole responsibility of, and DA shall have no liability towards You
                         |        for any consequences of such violation, including from the
                         |        suspension, termination of Your use of the Canton Service or from
                         |        the deletion of the Your Data.
                         |
                         |    9.  _NO SLA OR TECHNICAL SUPPORT_. Alpha products, including the Canton
                         |        Service, may not always perform as specified and can be changed from
                         |        time to time without notice. With respect to this alpha program, You
                         |        acknowledge and agree that DA has no obligation to provide You with
                         |        any technical support services in relation to the Canton Service
                         |        under this Agreement, and that no Service Level Agreement ("SLA") or
                         |        any service credits apply to Your use of the Canton Service,
                         |        including with respect to any downtime. Support, if any, will be
                         |        governed by a separate agreement between DA and You.
                         |
                         |   10.  _CHANGES TO THE SERVICE_. During the Alpha Term, DA may change or
                         |        discontinue the Canton Service at any time. DA is not obligated to
                         |        provide advance notice under this Section of any change or
                         |        discontinuation of the Canton Service.
                         |
                         |2.  FEES AND TAXES
                         |
                         |During the Alpha Term, the Canton Service is provided at no cost to You. Any
                         |future fees (the "FEES") for the Canton Service will be listed at
                         |[https://www.canton.io/fees.html] and may include different levels of fees based
                         |upon Your resource usage while accessing the Canton Service. DA can change the
                         |Fees from time to time and, by continuing to use the Canton Service after
                         |receiving notification of new Fees becoming effective, You agree to those Fees,
                         |as applicable. If any authority imposes any duty, tax, levy, or fee (excluding
                         |those based on DA's net income) on the Canton Service or any support services
                         |provided hereunder, You agree that You are responsible for and will pay any such
                         |amount and that You shall indemnify DA from and against any liability with
                         |respect to any amount imposed.
                         |
                         |3.  INTELLECTUAL PROPERTY
                         |
                         |    1.  _OWNERSHIP/TITLE_. As between DA and You, during and after the
                         |        term of this Agreement, (a) DA exclusively retains and owns all
                         |        right, title and interest, including but not limited to any and
                         |        all Intellectual Property Rights, in and to the Canton Service,
                         |        and (b) except as set forth in Section 3.2, You exclusively
                         |        retain and own all right, title and interest, including but not
                         |        limited to any and all Intellectual Property Rights, in and to
                         |        Your Data. "INTELLECTUAL PROPERTY RIGHTS" means any (i)
                         |        copyrights and copyrightable works, whether registered or
                         |        unregistered, (ii) trademarks, service marks, trade dress,
                         |        logos, registered designs, trade and business names (including
                         |        internet domain names, corporate names, and email address
                         |        names), whether registered or unregistered, (iii) patents,
                         |        patent applications, patent disclosures, mask works and
                         |        inventions (whether patentable or not), (iv) trade secrets,
                         |        know-how, data privacy rights, database rights, know-how, and
                         |        rights in designs, and (v) all other forms of intellectual
                         |        property or proprietary rights, in each case in every
                         |        jurisdiction worldwide. All rights not expressly granted to You
                         |        are retained by DA.
                         |
                         |    2.  _LICENSE_. You hereby authorize and grant DA a perpetual,
                         |        worldwide, royalty-free, non-exclusive, transferable, and
                         |        sub-licensable license to use, copy, distribute, display, modify
                         |        and prepare derivative works of Your Data, solely for the
                         |        purpose of DA providing the Canton Service to You in accordance
                         |        with this Agreement.
                         |
                         |    3.  _PURPOSE; RESTRICTIONS_. THE CANTON SERVICE IS PROVIDED BY DA TO
                         |        YOU DURING THE ALPHA TERM FOR EVALUATION PURPOSES ONLY. You may
                         |        use the Canton Service only during the Alpha Term, only for the
                         |        Purpose, and only within the Territory. ANY USE OF THE CANTON
                         |        SERVICE FOR ANYTHING OTHER THAN THE PURPOSE, INCLUDING USE IN
                         |        ANY PRODUCTION SETTING OR OUTSIDE OF THE TERRITORY, IS STRICTLY
                         |        PROHIBITED AND IS A MATERIAL BREACH OF THIS AGREEMENT. Without
                         |        limiting the foregoing, You will not and will not induce any
                         |        third party to, directly or indirectly: (a) copy, modify, create
                         |        a derivative work of, reverse engineer, decompile, translate,
                         |        disassemble, or otherwise attempt to extract any or all of the
                         |        source code of the Canton Service (except to the extent such
                         |        restriction is expressly prohibited by applicable law), (b) use
                         |        the Canton Service for any non-evaluation activity or outside of
                         |        the Territory; (c) use the Canton Service in any manner which
                         |        violates applicable law, including, without limitation, export
                         |        controls, broker dealer regulations, and banking
                         |        regulations, (d) sublicense, resell, or distribute any or all of
                         |        the Canton Service, (e) use the Canton Service in a manner
                         |        intended to avoid incurring Fees or exceed usage limits or
                         |        quotas, (f) use the Canton Service to operate or enable any
                         |        telecommunications service, (g) use the Canton Service to store
                         |        or transmit infringing, libelous, or otherwise unlawful or
                         |        tortious material, or to store or transmit material in violation
                         |        of third-party rights (including any materials which infringe
                         |        any third-party Intellectual Property Rights or are illegal,
                         |        obscene, indecent, defamatory, incite racial or ethnic hatred,
                         |        violate the rights of others, harm or threaten the safety of any
                         |        person or entity or may otherwise constitute a breach of any
                         |        applicable law), (h) use the Canton Service to store or transmit
                         |        Malicious Code; (i) intentionally or knowingly interfere with or
                         |        disrupt the integrity or performance of the Canton Service or
                         |        third-party data contained therein; (j) include any code in any
                         |        Custom Applications that You do not own or otherwise have the
                         |        right to use for such purpose; or (k) attempt to gain
                         |        unauthorized access to the Canton Service or their related
                         |        systems or networks.
                         |
                         |        "MALICIOUS CODE" means viruses, worms, time bombs, trojan horses and
                         |        other harmful or malicious code, files, scripts, agents or programs.
                         |
                         |    4.  _FEEDBACK_. You may provide comments, suggestions and other
                         |        evaluative observations or analysis regarding the Canton Service
                         |        (such items, collectively, "FEEDBACK"). Feedback shall be
                         |        confidential information of DA. You grant to DA, under Your
                         |        Intellectual Property Rights, a worldwide, fully paid, irrevocable
                         |        and non-exclusive license, with the right to sublicense to DA's
                         |        licensees and customers, the right to use and disclose Feedback in
                         |        any manner DA chooses and to display, perform, copy, make, have
                         |        made, use, sell, and otherwise dispose of DA's and its sublicensees
                         |        products embodying such Feedback in any manner and via any media DA
                         |        or its sublicensee chooses, without reference or obligation to You.
                         |        You will not provide DA any Feedback that (i) You have reason to
                         |        believe is subject to any patent, copyright, or other intellectual
                         |        property claim or right of any third party; or (ii) is subject to
                         |        license terms that seek to require any DA product incorporating or
                         |        derived from any Feedback, or other DA intellectual property, to be
                         |        licensed to or otherwise shared with any third party. Unless
                         |        otherwise set forth in this Agreement or authorized by You in
                         |        writing, DA shall not use or reference Your name in its use of such
                         |        Feedback.
                         |
                         |4.  REPRESENTATIONS AND DISCLAIMERS
                         |
                         |    1.  _REPRESENTATIONS_. Each Party represents and warrants that (a)
                         |        this Agreement is duly and validly executed and delivered by
                         |        such Party and constitutes a legal and binding obligation of
                         |        such Party, enforceable against such Party in accordance with
                         |        its terms; (b) such Party has all necessary power and authority
                         |        to execute and perform in accordance with this Agreement;
                         |        and (c) such Party's execution and performance of this Agreement
                         |        does not conflict with or violate any provision of law, rule or
                         |        regulation to which such Party is subject, or any agreement or
                         |        other obligation applicable to such Party or binding upon its
                         |        assets. If You are entering this Agreement on behalf of a legal
                         |        entity, You represent that You have the proper legal authority
                         |        to bind that entity to this Agreement. You also represent and
                         |        warrant that You own or have sufficient legal rights to upload
                         |        or use Your Data in connection with the Canton Service, and that
                         |        DA's use of Your Data will not violate any legal rights of any
                         |        third party, including any Intellectual Property Rights of any
                         |        third party.
                         |
                         |    2.  _DISCLAIMER OF WARRANTIES_. THE CANTON SERVICE IS AN ALPHA
                         |        VERSION NOT YET GENERALLY RELEASED AND IS PROVIDED "AS IS" WITH
                         |        NO REPRESENTATIONS AND WARRANTIES WHATSOEVER, EXPRESS OR
                         |        IMPLIED, AS TO THE CANTON SERVICE OR ANY TECHNICAL SUPPORT
                         |        SERVICES INCLUDING, BUT NOT LIMITED TO, ANY IMPLIED WARRANTIES
                         |        OR CONDITIONS OF MERCHANTABILITY, QUIET ENJOYMENT, SATISFACTORY
                         |        QUALITY, FITNESS FOR A PARTICULAR PURPOSE, AND TITLE, AND ANY
                         |        WARRANTY OR CONDITION OF NON-INFRINGEMENT. WITHOUT LIMITING THE
                         |        GENERALITY OF THE FOREGOING, NEITHER DA NOR ANY OF ITS LICENSORS
                         |        WARRANTS THAT THE CANTON SERVICE WILL PERFORM WITHOUT ERROR,
                         |        THAT IT WILL RUN WITHOUT IMMATERIAL INTERRUPTION OR THAT ANY
                         |        CONTENT AND DATA WILL BE SECURE OR NOT OTHERWISE LOST OR
                         |        DAMAGED. NEITHER DA NOR ANY OF ITS LICENSORS PROVIDES ANY
                         |        WARRANTY REGARDING, AND WILL HAVE NO RESPONSIBILITY FOR, ANY
                         |        CLAIM ARISING OUT OF A MODIFICATION OF THE CANTON SERVICE MADE
                         |        BY ANYONE OTHER THAN DA.
                         |
                         |    3.  _THIRD-PARTY MATERIALS_. DA ASSUMES NO LIABILITY FOR ANY
                         |        COMPUTER VIRUS OR SIMILAR CODE THAT IS DOWNLOADED TO ANY
                         |        COMPUTER OR OTHER DEVICE AS A RESULT OF YOUR USE OF THE CANTON
                         |        SERVICE. DA DOES NOT CONTROL, ENDORSE OR ACCEPT RESPONSIBILITY
                         |        FOR ANY THIRD-PARTY MATERIALS OR SERVICES OFFERED BY OR THROUGH
                         |        THE CANTON SERVICE.
                         |
                         |5.  LIMITATION OF LIABILITY
                         |
                         |Regardless of the basis on which You are entitled to claim damages from
                         |DA (including fundamental breach, negligence, misrepresentation, or
                         |other contract or tort claim), DA's entire liability for all claims in
                         |the aggregate arising from or related to the Canton Service or otherwise
                         |arising under this Agreement will not exceed the actual amount paid by
                         |You to DA under this Agreement during the twelve (12) months prior to
                         |the event giving rise to liability.
                         |
                         |IN NO EVENT WILL DA BE LIABLE TO YOU OR ANY OTHER PARTY FOR INDIRECT,
                         |INCIDENTAL, CONSEQUENTIAL, EXEMPLARY, PUNITIVE OR SPECIAL DAMAGES,
                         |INCLUDING, BUT NOT LIMITED TO, LOST PROFITS, BUSINESS GOODWILL OR
                         |ANTICIPATED SAVINGS, REGARDLESS OF THE FORM OF THE ACTION OR THE THEORY
                         |OF RECOVERY, EVEN IF DA HAS BEEN ADVISED OF THE POSSIBILITY OF THOSE
                         |DAMAGES.
                         |
                         |6.  INDEMNIFICATION
                         |
                         |Unless prohibited by applicable law, You will defend and indemnify DA
                         |and hold DA harmless from and against any and all claims, liabilities,
                         |damages, and expenses, including, without limitation, reasonable
                         |attorneys' and experts' fees, arising from or relating to Your use of
                         |the Canton Service or any acts, omissions, or misrepresentations under
                         |this Agreement, including but not limited to: (i) Your violation of DA's
                         |Acceptable Use Policy, (ii) any claim by a third party that Your Data
                         |(_e.g._, Your Custom Application(s)) violates the Intellectual Property
                         |Rights of a third party, and (iii) Your violation of any policy
                         |communicated to You by DA.
                         |
                         |7.  NOTICES
                         |
                         |All notices to DA must be in writing and will be deemed given only when
                         |sent by first class mail (return receipt requested), hand-delivered or
                         |sent by a nationally recognized overnight delivery service, to the DA
                         |address indicated at the beginning of this Agreement. All notices to You
                         |may be given electronically (_e.g._, to the email address submitted by
                         |the You upon registration). A Party may change its address for notices
                         |by notifying the other Party in writing.
                         |
                         |8.  GOVERNING LAW; JURY WAIVER
                         |
                         |THE AGREEMENT IS DEEMED TO BE MADE UNDER AND SHALL BE CONSTRUED
                         |ACCORDING TO THE LAWS OF THE STATE OF NEW YORK WITHOUT REFERENCE TO ITS
                         |CONFLICTS OF LAWS PROVISIONS. THE PARTIES HEREBY CONSENT TO THE
                         |EXCLUSIVE JURISDICTION OF THE STATE AND FEDERAL COURTS WITHIN THE
                         |BOROUGH OF MANHATTAN, CITY OF NEW YORK, FOR THE ADJUDICATION OF ALL
                         |MATTERS RELATING TO, OR ARISING UNDER THIS AGREEMENT AND THE PARTIES
                         |HEREBY IRREVOCABLY WAIVE, TO THE FULLEST EXTENT PERMITTED BY APPLICABLE
                         |LAW, ANY OBJECTION WHICH THEY MAY NOW OR HEREAFTER HAVE TO THE LAYING OF
                         |VENUE OF ANY SUCH PROCEEDING BROUGHT IN SUCH A COURT AND ANY CLAIM THAT
                         |ANY SUCH PROCEEDING BROUGHT IN SUCH COURT HAS BEEN BROUGHT IN ANY FORUM
                         |NON CONVENIENS. BOTH PARTIES AGREE TO WAIVE ANY RIGHT TO HAVE A JURY
                         |PARTICIPATE IN THE RESOLUTION OF THE DISPUTE OR CLAIM, BETWEEN ANY OF
                         |THE PARTIES OR ANY OF THEIR RESPECTIVE AFFILIATES RELATED TO IN ANY WAY
                         |THIS AGREEMENT. THIS AGREEMENT WILL NOT BE GOVERNED BY THE UNITED
                         |NATIONS CONVENTION ON CONTRACTS FOR THE INTERNATIONAL SALE OF GOODS, THE
                         |APPLICATION OF WHICH IS HEREBY EXPRESSLY EXCLUDED.
                         |
                         |9.  TERMINATION
                         |
                         |This Agreement is effective from the Effective Date until terminated in
                         |accordance with its terms (the "ALPHA TERM"). Either Party may terminate
                         |this Agreement for convenience, in whole or in part, any time by giving
                         |the other Party prior notice of the termination date. This Agreement
                         |shall automatically terminate upon Your acceptance, if any, of a
                         |successor agreement related to the Canton Service. The provisions
                         |relating to the following rights and obligations shall survive the
                         |termination, cancellation, expiration and/or rescission of this
                         |Agreement: Sections 1.4-1.7, 1.9, 2, 3.3, 3.4-6, and 8-10.
                         |
                         |Upon termination, You agree to promptly cease using the Canton Service
                         |and to, at your discretion, promptly backup Your Data from the Canton
                         |Service. DA reserves the right to delete Your Data from the Canton
                         |Service within a reasonable amount of time after termination of this
                         |Agreement.
                         |
                         |10.  GENERAL
                         |
                         |    1.  _INDEPENDENT CONTRACTORS_. DA and You will at all times be
                         |        independent contractors. Neither Party has any right, power or
                         |        authority to enter into any agreement for or on behalf of, or to
                         |        assume or incur any obligation or liabilities, express or
                         |        implied, on behalf of or in the name of, the other Party. This
                         |        Agreement will not be interpreted or construed to create an
                         |        association, joint venture or partnership between the Parties or
                         |        to impose any partnership obligation or liability upon either
                         |        Party.
                         |
                         |    2.  _YOUR INFORMATION_. Apart from account information, You shall
                         |        not disclose any Personal Information or Sensitive Data or
                         |        similarly protected or personal data to DA. If You disclose or
                         |        otherwise provide or furnish Personal Information or Sensitive
                         |        Data to DA, You represent and warrant that You have obtained all
                         |        the relevant consents to do so and will defend, indemnify and
                         |        hold DA harmless from and against any claims arising out of such
                         |        disclosure. To the full extent allowed by applicable law, You
                         |        agree that You are solely liable for all Personal Information
                         |        and Sensitive Data obligations, including, without limitation,
                         |        confidentiality and data protection and privacy obligations and
                         |        restrictions imposed by applicable law, regulation or court
                         |        order.
                         |
                         |        You agree to allow DA and its subsidiaries, Affiliates, subcontractors
                         |        and assignees to store, use, and transfer among such entities and
                         |        persons, any Personal Information or Sensitive Data provided by You,
                         |        including names, business phone numbers and business email addresses,
                         |        anywhere DA or any of such entities or persons does business worldwide,
                         |        provided that such information will be used only in connection with DA's
                         |        business relationship with You.
                         |
                         |    3.  _SEVERABILITY_. If any provision of this Agreement is held invalid
                         |        or unenforceable by a court of competent jurisdiction, the remaining
                         |        provisions of this Agreement remain in full force and effect. The
                         |        Parties agree to replace the invalid or unenforceable provision with
                         |        one that matches the original intent of the Parties.
                         |
                         |    4.  _THIRD-PARTY BENEFICIARIES_. For the avoidance of doubt, all rights
                         |        and benefits granted under this Agreement to You will be deemed
                         |        granted directly to You. Otherwise, no third party will be deemed to
                         |        be an intended or unintended third-party beneficiary of this
                         |        Agreement. DA is not responsible for any third-party claims against
                         |        You.
                         |
                         |    5.  _FORCE MAJEURE_. With the exceptions of confidentiality,
                         |        restrictions on use, and payment obligations, a Party will be
                         |        excused from the performance of its obligations under this Agreement
                         |        to the extent that such performance is prevented by Force Majeure
                         |        and the non-performing Party promptly provides notice of the
                         |        prevention to the other Party referencing this Section. Such excuse
                         |        shall be continued so long as the condition constituting force
                         |        majeure continues and the non-performing Party takes reasonable
                         |        efforts to remove the condition. "FORCE MAJEURE" means conditions
                         |        beyond the control of the affected Party, including an act of any
                         |        God, war, civil commotion, terrorist act, labor strike, failure or
                         |        default of public utilities or common carriers, fire, earthquake,
                         |        storm or like catastrophe, or failure or loss of utilities,
                         |        communications or computer (software and hardware) services
                         |        (provided that such failure could not have been prevented by the
                         |        exercise of skill, diligence, and prudence on the behalf of the
                         |        affected Party that would be reasonably and ordinarily expected from
                         |        a skilled and experienced person or entity engaged in the same type
                         |        of undertaking under the same or similar circumstances.
                         |
                         |    6.  _ENTIRE AGREEMENT, ASSIGNMENT_. This Agreement constitutes the
                         |        entire agreement of the Parties pertaining to the subject matter
                         |        hereof, and supersedes all prior agreements and understandings
                         |        pertaining thereto, notwithstanding any oral representations or
                         |        statements to the contrary heretofore made. This Agreement may not
                         |        be assigned or transferred by You without the prior written consent
                         |        of DA.
                         |
                         |    7.  _MODIFICATION; WAIVER; RIGHTS_. DA MAY AMEND OR RESTATE THIS
                         |        AGREEMENT AT ANY TIME UPON NOTICE TO YOU, WHICH NOTICE MAY BE GIVEN
                         |        ONLINE. BY CONTINUING TO ACCESS OR USE THE CANTON SERVICE, YOU AGREE
                         |        TO BE BOUND BY ANY SUCH AMENDMENTS MADE BY DA. No amendment of this
                         |        Agreement may be made by You unless in writing and signed by both
                         |        Parties. All rights and remedies provided for in this Agreement will
                         |        be cumulative and in addition to, and not in lieu of, any other
                         |        remedies available to either Party at law, in equity or otherwise.
                         |
                         |DA ACCEPTABLE USE POLICY
                         |
                         |Your use of the Canton Service is subject to this Acceptable Use Policy.
                         |Capitalized terms have the meaning given to them in the Agreement
                         |between You and DA.
                         |
                         |You agree not to, and not to allow third parties to use the Canton
                         |Service:
                         |
                         |1.  To violate, or encourage the violation of, the legal rights of
                         |    others (including, without limitation, storage or transmittal of any
                         |    material that violates the Intellectual Property Rights of any
                         |    entity or person);
                         |
                         |2.  To engage in, promote or encourage illegal activity;
                         |
                         |3.  To engage in the purchase, sale, or other transaction of any asset
                         |    or other item that would violate or cause DA to violate any
                         |    applicable law, including without limitation, the Securities
                         |    Exchange Act of 1934;
                         |
                         |4.  For any unlawful, invasive, infringing, defamatory or fraudulent
                         |    purpose (for example, this may include phishing, creating a pyramid
                         |    scheme or mirroring a website);
                         |
                         |5.  To store or transmit infringing, libelous, or otherwise unlawful or
                         |    tortious material, (including any materials which illegal, obscene,
                         |    indecent, defamatory, incites racial or ethnic hatred, violates the
                         |    rights of any entity or person, harms or threatens the safety of and
                         |    entity or person or may otherwise constitute a breach of any
                         |    applicable law)
                         |
                         |6.  To intentionally store or distribute Malicious Code or any items of
                         |    a destructive or deceptive nature;
                         |
                         |7.  To interfere with the use of the Canton Service, or the equipment
                         |    used to provide the Canton Service, by DA's other clients;
                         |
                         |8.  To disable, interfere with or circumvent any aspect of the Canton
                         |    Service;
                         |
                         |9.  To generate, distribute, publish or facilitate unsolicited emails,
                         |    promotions, advertisements or other solicitations; or
                         |
                         |10. To use the Canton Service, or any interfaces provided with the
                         |    Canton Service, to access any other products or services of DA or
                         |    its subcontractors in a manner that violates the terms of service of
                         |    such other product or service.
                         |
                         |
                         |""".stripMargin)

  def serviceAgreementStore(mk: () => ServiceAgreementStore): Unit = {

    val agreementText1 = String256M.tryCreate("1")
    val agreementText2 = String256M.tryCreate("2")

    "an empty service agreement store" should {

      "contain no accepted agreements" in {
        val sas = mk()
        for {
          agreements <- sas.listAcceptedAgreements(domain1)
          contains <- sas.containsAcceptedAgreement(domain1, agreement1)
        } yield assert(agreements.isEmpty && !contains)
      }

      "accept a non-existent agreement should fail" in {
        val sas = mk()
        val res = sas.insertAcceptedAgreement(domain1, agreement1)
        res.value.map(r => assert(r.isLeft))
      }
    }

    "store and get a single agreement" in {
      val sas = mk()

      for {
        _ <- sas.storeAgreement(domain1, agreement1, agreementText1).value
        text <- sas.getAgreement(domain1, agreement1).value
        contains <- sas.containsAgreement(domain1, agreement1)
      } yield assert(text.value == "1" && contains)
    }

    "store and get a large single agreement" in {
      val sas = mk()

      for {
        _ <- sas.storeAgreement(domain1, agreement1, largeAgreementText).value
        text <- sas.getAgreement(domain1, agreement1).value
        contains <- sas.containsAgreement(domain1, agreement1)
      } yield assert(text.value == largeAgreementText && contains)
    }

    "store and list multiple agreements" in {
      val sas = mk()

      for {
        _ <- sas.storeAgreement(domain1, agreement1, agreementText1).value
        _ <- sas.storeAgreement(domain1, agreement2, agreementText2).value
        _ <- sas.storeAgreement(domain2, agreement1, agreementText1).value
        _ <- sas.storeAgreement(domain2, agreement2, agreementText2).value
        agreements <- sas.listAgreements
      } yield agreements should have size 4
    }

    "insert and check for an accepted agreement for a single domain" in {
      val sas = mk()

      for {
        _ <- sas.storeAgreement(domain1, agreement1, agreementText1).value
        _ <- sas.insertAcceptedAgreement(domain1, agreement1).value
        contains <- sas.containsAcceptedAgreement(domain1, agreement1)
        acceptedAgreements <- sas.listAcceptedAgreements(domain1)
      } yield assert(acceptedAgreements.size == 1 && contains)
    }

    "insert and list accepted agreements for multiple domains" in {
      val sas = mk()

      for {
        _ <- sas.storeAgreement(domain1, agreement1, agreementText1).value
        _ <- sas.insertAcceptedAgreement(domain1, agreement1).value
        _ <- sas.storeAgreement(domain1, agreement2, agreementText2).value
        _ <- sas.insertAcceptedAgreement(domain1, agreement2).value
        _ <- sas.storeAgreement(domain2, agreement1, agreementText1).value
        _ <- sas.insertAcceptedAgreement(domain2, agreement1).value
        acceptedAgreements <- sas.listAcceptedAgreements(domain1)
      } yield acceptedAgreements should have size (2)
    }

    "acceptance of the same agreement and domain should be idempotent" in {
      val sas = mk()

      for {
        _ <- sas.storeAgreement(domain1, agreement1, agreementText1).value
        _ <- sas.insertAcceptedAgreement(domain1, agreement1).value
        _ <- sas.insertAcceptedAgreement(domain1, agreement1).value
        acceptedAgreements <- sas.listAcceptedAgreements(domain1)
      } yield acceptedAgreements should have size (1)
    }
  }
}
